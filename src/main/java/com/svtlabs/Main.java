package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point to the solitaire solver. The application relies on a C* database with the following
 * schema pre-created:
 *
 * <pre>
 * CREATE KEYSPACE solitaire WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
 * CREATE TABLE solitaire.boards (state blob PRIMARY KEY, best_result int, parents set<blob>, children set<blob>, client_id text, level tinyint);
 * CREATE TABLE solitaire.client_metrics (client_id text PRIMARY KEY, boards_processed counter);
 * CREATE TABLE solitaire.level_metrics (level tinyint PRIMARY KEY, boards_processed counter);
 * </pre>
 *
 * <p>The topic should be created like this:
 *
 * <pre>
 * bin/kafka-topics --zookeeper localhost --create --topic solitaire --partitions 100 --replication-factor 1
 * </pre>
 */
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  @NotNull private final CassandraClient cassandra;
  @NotNull private final KafkaClient kafka;
  private int consumeLevel;

  private Main(@NotNull String clientId) {
    cassandra = new CassandraClient(clientId);
    kafka = new KafkaClient();
  }

  private void processRecord(
      @NotNull ByteBuffer canonicalState, @Nullable ByteBuffer canonicalParent) {
    // Compute child moves and send to C*.

    // Check if this state is already stored in C*.
    PersistedBoard persistedBoard = cassandra.getPersistedBoard(canonicalState);
    if (persistedBoard != null) {
      // It already exists in C*, but its parents list might not contain the given parent.
      // Add it if necessary.
      if (canonicalParent != null && !persistedBoard.containsParent(canonicalParent)) {
        cassandra.addParent(canonicalState, canonicalParent);
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
    cassandra.storeBoard(stateBitSet, children, canonicalParent);

    // Send child tasks to Kafka, for any child that hasn't yet been explored. For those
    // that have been explored, add "current" as a parent of the child.
    if (children != null) {
      for (ByteBuffer child : children) {
        PersistedBoard persistedChildBoard = cassandra.getPersistedBoard(child);
        if (persistedChildBoard == null) {
          kafka.addTask(consumeLevel + 1, child, canonicalState);
        } else {
          LOGGER.debug("Child {} already exists in db", BitSet.valueOf(child));
          cassandra.addParent(child, canonicalState);
        }
      }
      kafka.flush();
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
    int processedCount = 0;
    //    for (int ctr = 0; ctr < 10; ++ctr) {
    while (processedCount < 500) {
      Collection<BoardTask> tasks = kafka.consumeTasks(consumeLevel);
      if (tasks.isEmpty()) {
        System.out.printf("Completed level %d!%n", consumeLevel);
        consumeLevel++;
        if (consumeLevel == Board.SLOTS) {
          // Completed last level. We're done!
          break;
        }
        continue;
      }
      for (BoardTask task : tasks) {
        processRecord(task.getState(), task.getParent());
        processedCount++;
      }
    }
  }

  private void close() {
    try {
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
    Main main = new Main(args[0]);

    // Get the ball rolling by pushing a task to Kafka (the initial state of the board)
    if ("ij1".equals(args[0])) {
      main.addInitialBoardTask();
    }

    main.run();

    //noinspection ConstantConditions
    if (false) {
      @SuppressWarnings("UnusedAssignment")
      List<Collection<PersistedBoard>> allBoards = main.cassandra.getAllPersistedBoards();
      Visualization.renderBoards(allBoards.get(0), 300, 100);
      Visualization.renderBoards(allBoards.get(1), 200, 300);
      Visualization.renderBoards(allBoards.get(2), 100, 500);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(main::close));
    main.close();
  }
}
