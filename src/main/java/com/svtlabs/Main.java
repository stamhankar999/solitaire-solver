package com.svtlabs;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.svtlabs.Metrics.Context;
import com.svtlabs.jedis.RedisClient;
import com.svtlabs.jedis.RedisClient.BatchWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point to the solitaire solver. The application relies on a C* database with the schema
 * specified in misc/schema.cql pre-created.
 */
public class Main {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);
  @NotNull private final CassandraClient cassandra;
  @NotNull private final RedisClient redis;
  @NotNull private final AtomicBoolean stopped;
  @NotNull private final CountDownLatch doneStopping;
  @NotNull private final AtomicInteger cassInProgress;

  private Main(
      @NotNull String clientId,
      @NotNull String redisTarget,
      @NotNull String cassandraSeed,
      int maxBoardsInTask) {
    cassandra = new CassandraClient(cassandraSeed);
    redis = new RedisClient(clientId, redisTarget, maxBoardsInTask);
    stopped = new AtomicBoolean();
    doneStopping = new CountDownLatch(1);
    cassInProgress = new AtomicInteger(0);
  }

  private void processCassStage(Board b, CompletionStage<? extends AsyncResultSet> stage) {
    stage.whenComplete(
        (result, excp) -> {
          if (excp != null) {
            LOGGER.error("Error writing to C*", excp);
            redis.error(b.getState());
          }
          cassInProgress.decrementAndGet();
        });
  }

  private void processBoard(@NotNull Board b, Metrics metrics, BatchWriter redisWriter)
      throws ExecutionException, InterruptedException {
    // Compute child moves and send to Redis/C*.

    // Check if this is a winning board.
    if (b.getLevel() == Board.SLOTS - 1) {
      LOGGER.info("We have a winner!");

      // Record the winning board in C*.
      cassInProgress.incrementAndGet();
      metrics.measure(
          "cass.prep", () -> processCassStage(b, cassandra.storeWinningBoard(b.getState())));
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

    // Add child->parent mappings to C*, and add child tasks to Redis.
    ByteBuffer canonicalState = ByteBuffer.wrap(b.getState().toByteArray());
    if (children != null) {
      for (ByteBuffer child : children) {
        cassInProgress.incrementAndGet();
        metrics.measure(
            "cass.prep",
            () -> processCassStage(b, cassandra.addBoardRelation(child, canonicalState)));
        metrics.measure("redis.prep", () -> redisWriter.addTask(child));
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
    try {
      Metrics metrics = new Metrics();
      List<Board> task = new ArrayList<>();
      long lastLogTime = 0L;
      int numTasks = 0;
      while (!stopped.get()) {
        task.clear();
        numTasks++;
        Context taskTimer = metrics.start("totalTime");
        ByteBuffer rawTask = metrics.measure("taskPull", () -> redis.nextTask(task));
        int maxLevel = 0;
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
        for (Map.Entry<Board, Boolean> entry : boardExists.entrySet()) {
          Board board = entry.getKey();
          Boolean exists = entry.getValue();
          maxLevel = Math.max(board.getLevel(), maxLevel);
          if (exists) {
            metrics.measure("redis.prep", () -> redisWriter.completed(board, true));
            metrics.incrBy("alreadySeen", 1);
          } else {
            processBoard(board, metrics, redisWriter);
            metrics.measure("redis.prep", () -> redisWriter.completed(board, false));
          }
        }

        redisWriter.execute(metrics, rawTask);

        // We're done with this task. Stop its timer.
        taskTimer.stop();

        // Log the final stats.
        if (LOGGER.isInfoEnabled()) {
          if (System.currentTimeMillis() - lastLogTime > 100) {
            lastLogTime = System.currentTimeMillis();

            // Record the max-level seen in the metrics, and the current number of outstanding C*
            // updates.
            metrics.incrBy("maxLevel", maxLevel);
            metrics.incrBy("cass.inProgress", cassInProgress.get());
            metrics.incrBy("numTasks", numTasks);
            LOGGER.info(metrics.toString());
            metrics.clear();
            numTasks = 0;
          }
        }
      }

      LOGGER.info("Application is preparing to shut down.");
      while (cassInProgress.get() > 0) {
        LOGGER.info("Waiting for {} outstanding C* ops to complete...", cassInProgress.get());
        Thread.sleep(250);
      }
    } finally {
      LOGGER.info("C* ops are complete; shutting down.");
      doneStopping.countDown();
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
    try {
      doneStopping.await();
    } catch (InterruptedException e) {
      LOGGER.warn("Interrupted while waiting for graceful shutdown!");
    }
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
