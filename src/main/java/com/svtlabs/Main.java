package com.svtlabs;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
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
  @NotNull private final KafkaClient kafka;
  @NotNull private final AtomicBoolean stopped;

  private Main(
      @NotNull String clientId,
      @NotNull String bootstrapServers,
      @NotNull String topicName,
      @NotNull String cassandraSeed) {
    cassandra = new CassandraClient(clientId, cassandraSeed);
    kafka = new KafkaClient(clientId, bootstrapServers, topicName);
    stopped = new AtomicBoolean();
  }

  private void processRecord(@NotNull ByteBuffer canonicalState, Map<String, Long> metrics)
      throws ExecutionException, InterruptedException {
    // Compute child moves and send to C*.

    // Check if this state is already stored in C*.
    long start = System.currentTimeMillis();
    Board persistedBoard = cassandra.getBoard(canonicalState);
    metrics.put("boardCheck", System.currentTimeMillis() - start);
    if (persistedBoard != null) {
      LOGGER.info(
          "Encountered board that has already been processed at level {}!",
          persistedBoard.getLevel());
      return;
    }

    Board b = new Board(canonicalState);

    // Check if this is a winning board.
    if (b.getLevel() == Board.SLOTS - 1) {
      LOGGER.info("We have a winner!");
      List<CompletionStage<? extends AsyncResultSet>> futures = new ArrayList<>();
      long startCass = System.currentTimeMillis();
      futures.add(cassandra.storeWinningBoard(b.getState()));
      futures.addAll(cassandra.storeBoard(b.getState()));
      // Wait for all C* ops to complete.
      futures.forEach(stage -> stage.toCompletableFuture().join());
      metrics.put("cassUpdate", System.currentTimeMillis() - startCass);
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
    long kafkaTime = 0;
    if (children != null) {
      List<CompletionStage<? extends AsyncResultSet>> futures = new ArrayList<>();
      for (ByteBuffer child : children) {
        long startChild = System.currentTimeMillis();
        futures.add(cassandra.addBoardRelation(child, canonicalState));
        long doneRelation = System.currentTimeMillis();
        kafka.addTask(child);
        long doneKafka = System.currentTimeMillis();

        // Accumulate metrics.
        cassTime += (doneRelation - startChild);
        kafkaTime += (doneKafka - doneRelation);
      }
      long startCass = System.currentTimeMillis();
      futures.addAll(cassandra.storeBoard(b.getState()));
      long endCass = System.currentTimeMillis();
      cassTime += (endCass - startCass);

      // Flush (asynchronously) to Kafka.
      CompletableFuture<Long> kafkaFlushFuture =
          CompletableFuture.supplyAsync(
              () -> {
                kafka.flush();
                return System.currentTimeMillis() - endCass;
              });

      // Wait for all C* ops to complete.
      startCass = System.currentTimeMillis();
      futures.forEach(stage -> stage.toCompletableFuture().join());
      cassTime += (System.currentTimeMillis() - startCass);

      // Now wait for the flush to complete.
      Long flushTime = kafkaFlushFuture.get();
      kafkaTime += flushTime;

      metrics.put("cassUpdate", cassTime);
      metrics.put("kafkaUpdate", kafkaTime);
    }
  }

  private void run() throws ExecutionException, InterruptedException {
    // Now consume "tasks" from Kafka, where each task involves the following:
    // * Calculate the list of possible next moves from the current board.
    // * Add rows in C* with the child,current mappings.
    // * Add a row in C* for the current board, to declare that it's done processing.
    // * For each child, add a task in Kafka.
    //
    // processRecord does most of the work.
    Map<String, Long> metrics = new LinkedHashMap<>();
    //    for (int ctr = 0; ctr < 100 && !stopped.get(); ++ctr) {
    //    while (processedCount < 500 && !stopped) {
    while (!stopped.get()) {
      metrics.clear();
      long start = System.currentTimeMillis();
      Collection<BoardTask> tasks = kafka.consumeTasks();
      metrics.put("kafkaPull", System.currentTimeMillis() - start);
      if (tasks.isEmpty()) {
        // No tasks found; try again!
        continue;
      }
      for (BoardTask task : tasks) {
        // TODO: This is actually wrong if there are multiple tasks, since metrics values are
        // replaced in each
        // iteration. Also, if there are multiple tasks, we wouldn't want to join on the async tasks
        // in processRecord.
        // We'd want to collect all the futures here and then join the accumulated futures across
        // tasks.
        processRecord(task.getState(), metrics);
      }
      kafka.commitAsync();
      // Log the final stats.
      if (LOGGER.isInfoEnabled()) {
        StringBuilder sb = new StringBuilder();
        sb.append("taskCount: ").append(tasks.size());
        metrics.forEach((name, value) -> sb.append("  ").append(name).append(": ").append(value));
        LOGGER.info(sb.toString());
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

  private void stop() {
    stopped.set(true);
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    if (args.length != 4) {
      System.err.println(
          "Usage: solitaire-solver <client-id> <kafka-servers> <topic-name> <c*-seed>");
    }
    Main main = new Main(args[0], args[1], args[2], args[3]);
    try {
      Runtime.getRuntime().addShutdownHook(new Thread(main::stop));
      main.run();
    } finally {
      main.close();
    }
  }
}
