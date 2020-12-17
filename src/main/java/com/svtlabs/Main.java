package com.svtlabs;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.svtlabs.jedis.BoardTask;
import com.svtlabs.jedis.RedisClient;
import com.svtlabs.jedis.RedisClient.BatchWriter;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
      @NotNull String clientId, @NotNull String redisTarget, @NotNull String cassandraSeed) {
    cassandra = new CassandraClient(clientId, cassandraSeed);
    redis = new RedisClient(clientId, redisTarget);
    stopped = new AtomicBoolean();
  }

  private long completeTask(Board b) {
    long startRedis = System.currentTimeMillis();
    BatchWriter redisWriter = redis.createBatchWriter();
    redisWriter.completed(b.getState());
    redisWriter.execute();
    return System.currentTimeMillis() - startRedis;
  }

  private void processRecord(@NotNull ByteBuffer canonicalState, Map<String, Long> metrics)
      throws ExecutionException, InterruptedException {
    // Compute child moves and send to Redis/C*.

    // Check if this state has already been processed.
    long start = System.currentTimeMillis();
    boolean found = redis.boardExists(canonicalState);
    metrics.put("boardCheck", System.currentTimeMillis() - start);
    if (found) {
      Board b = new Board(canonicalState);
      LOGGER.info("Encountered board that has already been processed at level {}!", b.getLevel());
      metrics.put("redisUpdate", completeTask(b));
      return;
    }

    Board b = new Board(canonicalState);

    // Check if this is a winning board.
    if (b.getLevel() == Board.SLOTS - 1) {
      LOGGER.info("We have a winner!");
      // Record this state as completed in redis + stats.
      CompletableFuture<Long> redisTimeFuture =
          CompletableFuture.supplyAsync(() -> completeTask(b));

      // Record the winning board in C*.
      long startCass = System.currentTimeMillis();
      cassandra.storeWinningBoard(b.getState()).toCompletableFuture().join();
      metrics.put("cassUpdate", System.currentTimeMillis() - startCass);
      metrics.put("redisUpdate", redisTimeFuture.get());
      return;
    }

    Set<ByteBuffer> children = null;
    for (Board child : b) {
      if (children == null) {
        children = new LinkedHashSet<>();
      }
      children.add(ByteBuffer.wrap(MoveHelper.canonicalize(child.getState()).toByteArray()));
    }

    // Add child->parent mappings to C*, and add child tasks to Kafka.
    long cassTime = 0;
    long redisTime = 0;
    BatchWriter redisWriter = redis.createBatchWriter();
    if (children != null) {
      List<CompletionStage<? extends AsyncResultSet>> futureStages = new ArrayList<>();
      for (ByteBuffer child : children) {
        long startChild = System.currentTimeMillis();
        futureStages.add(cassandra.addBoardRelation(child, canonicalState));
        long doneRelation = System.currentTimeMillis();
        redisWriter.addTask(child);
        long doneRedis = System.currentTimeMillis();

        // Accumulate metrics.
        cassTime += (doneRelation - startChild);
        redisTime += (doneRedis - doneRelation);
      }

      long asyncCassStartTime = System.currentTimeMillis();
      List<CompletableFuture<? extends AsyncResultSet>> futures =
          futureStages
              .stream()
              .map(CompletionStage<? extends AsyncResultSet>::toCompletableFuture)
              .collect(Collectors.toList());
      CompletableFuture<Long> asyncCassFinishTimeFuture =
          CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
              .thenApply(
                  (x) -> {
                    return System.currentTimeMillis();
                  });

      long startCompleted = System.currentTimeMillis();
      redisWriter.completed(b.getState());
      redisWriter.execute();
      long endCompleted = System.currentTimeMillis();
      redisTime += (endCompleted - startCompleted);

      // Wait for all C* ops to complete.
      long asyncCassFinishTime = asyncCassFinishTimeFuture.get();
      cassTime += (asyncCassFinishTime - asyncCassStartTime);
    } else {
      // Terminal state, but not a winner.
      long startCompleted = System.currentTimeMillis();
      redisWriter.completed(b.getState());
      redisWriter.execute();
      long endCompleted = System.currentTimeMillis();
      redisTime += (endCompleted - startCompleted);
    }

    metrics.put("cassUpdate", cassTime);
    metrics.put("redisUpdate", redisTime);
  }

  private void run() throws ExecutionException, InterruptedException {
    // Now consume "tasks" from Redis, where each task involves the following:
    // * Calculate the list of possible next moves from the current board.
    // * Add rows in C* with the child,current mappings.
    // * Add a key in Redis for the current board, to declare that it's done processing.
    // * For each child, add a task in Redis.
    //
    // processRecord does most of the work.
    Map<String, Long> metrics = new LinkedHashMap<>();
    //    for (int ctr = 0; ctr < 100 && !stopped.get(); ++ctr) {
    //    while (processedCount < 500 && !stopped) {
    while (!stopped.get()) {
      metrics.clear();
      long start = System.currentTimeMillis();
      BoardTask task = redis.nextTask();
      if (task == null) {
        // No task found. Loop around and try again.
        continue;
      }
      metrics.put("taskPull", System.currentTimeMillis() - start);
      processRecord(task.getState(), metrics);

      // Log the final stats.
      if (LOGGER.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append("totalTime: ").append(System.currentTimeMillis() - start);
        metrics.forEach((name, value) -> sb.append("  ").append(name).append(": ").append(value));
        LOGGER.info(sb.toString());
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
    if (args.length != 3) {
      System.err.println("Usage: solitaire-solver <client-id> <redis-target> <c*-seed>");
      System.exit(1);
    }
    Main main = new Main(args[0], args[1], args[2]);
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(main::stop));
      main.run();
    } finally {
      main.close();
    }
  }
}
