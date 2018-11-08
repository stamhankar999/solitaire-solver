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

  private void processRecord(@NotNull ByteBuffer state, @Nullable ByteBuffer parent) {
    // Compute child moves and send to C*.
    // TODO: If a child is a rotational equivalent of another child, exclude one.

    // Check if this state is already stored in C*.
    PersistedBoard persistedBoard = cassandra.getPersistedBoard(state);
    if (persistedBoard != null) {
      // It already exists in C*, but its parents list might not contain the given parent.
      // Add it if necessary.
      if (parent != null && !persistedBoard.containsParent(parent)) {
        cassandra.addParent(state, parent);
      }
      return;
    }

    BitSet stateBitSet = BitSet.valueOf(state);
    Board b = new Board(stateBitSet, consumeLevel);
    Set<ByteBuffer> children = null;
    for (Board child : b) {
      if (children == null) {
        children = new LinkedHashSet<>();
      }
      children.add(ByteBuffer.wrap(child.getState().toByteArray()));
    }
    cassandra.storeBoard(stateBitSet, children, parent);

    // Send child tasks to Kafka, for any child that hasn't yet been explored. For those
    // that have been explored, add "current" as a parent of the child.
    // TODO: If a child is a rotational equivalent of another child (in this set of
    // children), skip it. If it's equivalent to a child in the db, update that child to include
    // this child's parent.
    if (children != null) {
      for (ByteBuffer child : children) {
        PersistedBoard persistedChildBoard = cassandra.getPersistedBoard(child);
        if (persistedChildBoard == null) {
          kafka.addTask(consumeLevel + 1, child, state);
        } else {
          LOGGER.debug("Child {} already exists in db", BitSet.valueOf(child));
          cassandra.addParent(child, state);
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
    while (processedCount < 1000) {
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

  @SuppressWarnings("unused")
  private void renderBoardAncestry(@NotNull ByteBuffer board, int level) {
    PersistedBoard persistedBoard = cassandra.getPersistedBoard(board);
    assert persistedBoard != null;
    Set<ByteBuffer> persistedParents = persistedBoard.getParents();

    if (persistedParents != null && !persistedParents.isEmpty()) {
      renderBoardAncestry(persistedParents.iterator().next(), level + 1);
    }
    BitSet state = BitSet.valueOf(board);
    RenderedBoard renderedBoard = new RenderedBoard(String.format("sol %d", level), state);
    renderedBoard.setLocation(level % 9 * 150, 100 + 200 * (level / 9));
    renderedBoard.setVisible(true);
  }

  @SuppressWarnings("unused")
  private void renderBoards(@NotNull Collection<PersistedBoard> boards, int startX, int startY) {
    int idx = 0;
    for (PersistedBoard board : boards) {
      RenderedBoard renderedBoard =
          new RenderedBoard(String.format("sol %d", idx), BitSet.valueOf(board.getState()));
      renderedBoard.setLocation(startX + idx % 9 * 150, startY + 200 * (idx / 9));
      renderedBoard.setVisible(true);
      idx++;
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
      main.renderBoards(allBoards.get(0), 300, 100);
      main.renderBoards(allBoards.get(1), 200, 300);
      main.renderBoards(allBoards.get(2), 100, 500);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(main::close));
    main.close();
  }
}
