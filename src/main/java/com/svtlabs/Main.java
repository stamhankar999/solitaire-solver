package com.svtlabs;

import com.svtlabs.Metrics.Context;
import com.svtlabs.jedis.RedisClient;
import com.svtlabs.jedis.RedisClient.BatchWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.jetbrains.annotations.NotNull;
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
  @NotNull private final CassandraClient cassandra;
  @NotNull private final RedisClient redis;
  @NotNull private final AtomicBoolean stopped;

  private Main(
      @NotNull String clientId,
      @NotNull String redisTarget,
      @NotNull String cassandraSeed,
      int maxBoardsInTask) {
    cassandra = new CassandraClient(clientId, cassandraSeed);
    redis = new RedisClient(clientId, redisTarget, maxBoardsInTask);
    stopped = new AtomicBoolean();
  }

  private void processBoard(
      @NotNull Board b,
      Metrics metrics,
      BatchWriter redisWriter,
      List<CompletableFuture<?>> cassFutures)
      throws ExecutionException, InterruptedException {
    // Compute child moves and send to Redis/C*.

    // Check if this is a winning board.
    if (b.getLevel() == Board.SLOTS - 1) {
      LOGGER.info("We have a winner!");

      // Record the winning board in C*.
      cassFutures.add(
          metrics.measure(
              "cass.prep", () -> cassandra.storeWinningBoard(b.getState()).toCompletableFuture()));
      return;
    }

    Set<ByteBuffer> children =
        metrics.measure(
            "compute",
            () -> {
              Set<ByteBuffer> result = null;
              for (Board child : b) {
                if (result == null) {
                  result = new LinkedHashSet<>();
                }
                result.add(
                    ByteBuffer.wrap(MoveHelper.canonicalize(child.getState()).toByteArray()));
              }
              return result;
            });

    // Add child->parent mappings and child tasks to Redis.
    ByteBuffer canonicalState = ByteBuffer.wrap(b.getState().toByteArray());
    if (children != null) {
      for (ByteBuffer child : children) {
        metrics.measure(
            "redis.prep",
            () -> {
              redisWriter.addTask(child);
              redisWriter.addBoardRelation(child, canonicalState);
            });
      }
    }
  }

  private void run() throws ExecutionException, InterruptedException {
    // Now consume "tasks" from Redis, where each task involves the following:
    // * Calculate the list of possible next moves from the current board.
    // * Add rows in C* with the child,current mappings.
    // * Add a key in Redis for the current board, to declare that it's done processing.
    // * For each child, add a task in Redis.
    //
    // processRecord does most of the work.
    Metrics metrics = new Metrics();
    List<Board> task = new ArrayList<>();
    while (!stopped.get()) {
      metrics.clear();
      task.clear();
      Context taskTimer = metrics.start("totalTime");
      ByteBuffer rawTask = metrics.measure("taskPull", () -> redis.nextTask(task));
      if (rawTask == null) {
        // No task found. Loop around and try again.
        continue;
      }

      // Save a metric for the number of boards in this task.
      metrics.incrBy("numBoards", task.size());

      // Check if these boards have been previously processed.
      Map<Board, Boolean> boardExists =
          metrics.measure("boardsCheck", () -> redis.checkExistence(task));

      BatchWriter redisWriter = redis.createBatchWriter();
      List<CompletableFuture<?>> cassFutures = new ArrayList<>();
      Context cassUpdateTimer = metrics.start("cass.update");
      for (Map.Entry<Board, Boolean> entry : boardExists.entrySet()) {
        Board board = entry.getKey();
        Boolean exists = entry.getValue();
        if (exists) {
          LOGGER.info(
              "Encountered board that has already been processed at level {}!", board.getLevel());
          metrics.measure("redis.prep", () -> redisWriter.completed(board, true));
        } else {
          processBoard(board, metrics, redisWriter, cassFutures);
          metrics.measure("redis.prep", () -> redisWriter.completed(board, false));
        }
      }

      // Once all the cass futures are done, stop the cass-time timer.
      CompletableFuture<Void> cassFinishFuture =
          CompletableFuture.allOf(cassFutures.toArray(new CompletableFuture[0]))
              .thenAccept((x) -> cassUpdateTimer.stop());

      redisWriter.execute(metrics, rawTask);

      // Wait for all C* ops to complete.
      cassFinishFuture.join();

      // We're done with this task. Stop its timer.
      taskTimer.stop();

      // Log the final stats.
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info(metrics.toString());
      }
    }
  }

  private void close() {
    try {
      cassandra.close();
      redis.close();
    } catch (RuntimeException e) {
      // swallow
    }
  }

  private void stop() {
    stopped.set(true);
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    if (args.length != 4) {
      System.err.println(
          "Usage: solitaire-solver <client-id> <redis-target> <c*-seed> <max-boards-in-task>");
      System.exit(1);
    }
    Main main = new Main(args[0], args[1], args[2], Integer.parseInt(args[3]));
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(main::stop));
      main.run();
    } finally {
      main.close();
    }
  }
}
