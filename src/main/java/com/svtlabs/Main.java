package com.svtlabs;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point to the solitaire solver. The application relies on a C* database with the schema
 * specified in misc/schema.cql pre-created. It also relies on the following topic to exist in
 * Kafka:
 *
 * <pre>
 * bin/kafka-topics --zookeeper localhost --create --topic solitaire --partitions 100 --replication-factor 1
 * </pre>
 */
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  public static final int MAX_LEVEL = 8;
  @NotNull private final CassandraClient cassandra;
  @NotNull private final KafkaClient kafka;
  private int consumeLevel;
  private final ExecutorService executorService;

  private Main(@NotNull String clientId, int numThreads) {
    cassandra = new CassandraClient(clientId, "192.168.127.121");
    kafka = new KafkaClient();
    executorService = Executors.newFixedThreadPool(numThreads);
  }

  private void processRecord(
      @NotNull ByteBuffer canonicalState, @Nullable ByteBuffer canonicalParent) {
    // Check if this state is already stored in C*.
    PersistedBoard persistedBoard = cassandra.getPersistedBoard(canonicalState);
    if (persistedBoard != null) {
      // It already exists in C*, but its parents list might not contain the given parent.
      // Add it if necessary.
      if (canonicalParent != null && !persistedBoard.containsParent(canonicalParent)) {
        CompletionStage<? extends AsyncResultSet> stage =
            cassandra.addParentAsync(canonicalState, canonicalParent);
        cassandra.updateBestResult(canonicalParent, persistedBoard.getBestResult());
        stage.toCompletableFuture().join();
      }
      return;
    }

    BitSet stateBitSet = BitSet.valueOf(canonicalState);
    Board b = new Board(stateBitSet, consumeLevel);
    Set<ByteBuffer> children = null;
    for (Board child : b) {
      if (children == null) {
        children = new LinkedHashSet<>();
      }
      children.add(ByteBuffer.wrap(MoveHelper.canonicalize(child.getState()).toByteArray()));
    }
    // First, begin adding the parent.
    ByteBuffer stateBuffer = ByteBuffer.wrap(stateBitSet.toByteArray());
    CompletableFuture<? extends AsyncResultSet> addParentFuture = canonicalParent != null ?
        cassandra.addParentAsync(stateBuffer, canonicalParent).toCompletableFuture() : null;

    cassandra.storeBoard(stateBitSet, children);

    // Send child tasks to Kafka, for any child that hasn't yet been explored. For those
    // that have been explored, add "current" as a parent of the child. Propagate best_result's
    // from children to "current" and beyond as needed. There can be many children, so we make
    // async queries to C* to get the persisted-boards and process them.
    if (children != null) {
      List<CompletionStage<Void>> stages =
          children
              .stream()
              .map(
                  child ->
                      cassandra
                          .getPersistedBoardAsync(child)
                          .thenAccept(
                              persistedChildBoard -> {
                                if (persistedChildBoard == null) {
                                  if (addParentFuture != null) {
                                    addParentFuture.join();
                                  }
                                  kafka.addTask(consumeLevel + 1, child, canonicalState);
                                } else {
                                  LOGGER.debug(
                                      "Child {} already exists in db", BitSet.valueOf(child));
                                  CompletionStage<? extends AsyncResultSet> stage =
                                      cassandra.addParentAsync(child, canonicalState);
                                  if (addParentFuture != null) {
                                    addParentFuture.join();
                                  }
                                  cassandra.updateBestResult(
                                      canonicalState, persistedChildBoard.getBestResult());
                                  stage.toCompletableFuture().join();
                                }
                              }))
              .collect(Collectors.toList());

      // Wait for all of the children to be processed and then flush the pending kafka
      // messages (for submitting new tasks to the queue).
      stages.forEach(stage -> stage.toCompletableFuture().join());
      if (addParentFuture != null) {
        addParentFuture.join();
      }
      kafka.flush();
    } else {
      // This is a leaf state, so the game ends (for this path) with however many pegs are left.
      if (addParentFuture != null) {
        addParentFuture.join();
      }
      cassandra.updateBestResult(canonicalState, (byte) stateBitSet.cardinality());
    }
  }

  private void addInitialBoardTask() {
    Board initial = Board.initial();
    kafka.addTask(1, ByteBuffer.wrap(initial.getState().toByteArray()), null);
    consumeLevel = 1;
    kafka.flush();
  }

  private void run() {
    // Now consume "tasks" from Kafka, where each task involves the following:
    // * Calculate the list of possible next moves from the current board.
    // * Add a row in C* with the current board, its children, and its parent.
    // * For each child, add a task in Kafka.

    //    for (int ctr = 0; ctr < 10; ++ctr) {
    // Create an execution pool to push processRecord tasks into.
    while (true) {
      Collection<BoardTask> tasks = kafka.consumeTasks(consumeLevel);
      if (tasks.isEmpty()) {
        System.out.printf("Completed level %d!%n", consumeLevel);
        consumeLevel++;
        if (consumeLevel == MAX_LEVEL) { // Board.SLOTS) {
          // Completed last level. We're done!
          break;
        }
        continue;
      }

      List<Future<?>> futures = new ArrayList<>();
      for (BoardTask task : tasks) {
        // Add this task to the execution queue and collect the future.
        futures.add(executorService.submit(() -> processRecord(task.getState(), task.getParent())));
      }

      // Wait on futures.
      futures.forEach(
          f -> {
            try {
              f.get();
            } catch (InterruptedException | ExecutionException e) {
              e.printStackTrace();
            }
          });
    }
  }

  private void close() {
    try {
      executorService.shutdown();
      cassandra.close();
      kafka.close();
    } catch (RuntimeException e) {
      // swallow
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: solitaire-solver <client-id>");
    }
    Main main = new Main(args[0], 1);

    // Get the ball rolling by pushing a task to Kafka (the initial state of the board)
    if ("ij1".equals(args[0])) {
      main.addInitialBoardTask();
    }

    long start = System.currentTimeMillis();
    main.run();
    long end = System.currentTimeMillis();
    System.out.println(String.format("Run time: %d ms", end - start));

    //noinspection ConstantConditions
    if (false) {
      @SuppressWarnings("UnusedAssignment")
      List<Collection<PersistedBoard>> allBoards = main.cassandra.getAllPersistedBoardsByLevel();
      Visualization.renderBoards(allBoards.get(0), 300, 100);
      Visualization.renderBoards(allBoards.get(1), 200, 300);
      Visualization.renderBoards(allBoards.get(2), 100, 500);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(main::close));
    main.close();
  }
}
