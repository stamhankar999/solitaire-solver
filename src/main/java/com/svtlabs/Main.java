package com.svtlabs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
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
  //  private List<Map<Board, Integer>> seenBoards = new ArrayList<>();
  //  ExecutorService executorService = Executors.newFixedThreadPool(7);
  //  private final ScheduledExecutorService scheduler;
  private static final String TOPIC_BASE = "solitaire";
  private static final String GROUP_ID = "Solitaire";
  private static final String CLIENT_ID = "Solitaire";
  private static final int SLOTS = 37;
  private final PreparedStatement insertStatement;
  private final PreparedStatement selectStatement;
  private final PreparedStatement clientMetricsStatement;
  private final PreparedStatement levelMetricsStatement;
  private final PreparedStatement updateParentsStatement;

  private final CqlSession session;
  private final KafkaProducer<ByteBuffer, ByteBuffer> producer;
  private final ConsumerWithTopic<ByteBuffer, ByteBuffer> consumer;
  private final String clientId;

  private int consumeLevel;

  private Main(String clientId) throws UnknownHostException {
    this.clientId = clientId;

    // Connect to C*.
    session = new CqlSessionBuilder().build();
    insertStatement =
        session.prepare(
            "INSERT INTO solitaire.boards (state, children, client_id, level) VALUES (:state, :children, :client_id, :level)");
    updateParentsStatement =
        session.prepare(
            "UPDATE solitaire.boards SET parents = parents + :new_parent WHERE state = :state");
    selectStatement = session.prepare("SELECT * FROM solitaire.boards WHERE state = :state");
    clientMetricsStatement =
        session.prepare(
            "UPDATE solitaire.client_metrics SET boards_processed = boards_processed + 1 WHERE client_id = :client_id");
    levelMetricsStatement =
        session.prepare(
            "UPDATE solitaire.level_metrics SET boards_processed = boards_processed + 1 WHERE level = :level");

    // Connect to Kafka and create our producer and consumer objects.
    Properties producerProps = new Properties();
    producerProps.put("client.id", CLIENT_ID);
    producerProps.put("bootstrap.servers", "localhost:9092");
    producerProps.put("retries", "2");
    producerProps.put("acks", "all");
    producerProps.put("key.serializer", ByteBufferSerializer.class);
    producerProps.put("value.serializer", ByteBufferSerializer.class);
    producer = new KafkaProducer<>(producerProps);

    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("key.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("value.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("group.id", GROUP_ID);
    consumerProps.put("client.id", CLIENT_ID);
    consumerProps.put("auto.offset.reset", "earliest");
    //    props.put("max.poll.records", "10");

    // Create the consumer using props.
    consumer = new ConsumerWithTopic<>(new KafkaConsumer<>(consumerProps));

    //    for (int i = 0; i < SLOTS; ++i) {
    //      seenBoards.add(new ConcurrentHashMap<>());
    //    }
    //    scheduler = Executors.newScheduledThreadPool(1);
    //    scheduler.scheduleAtFixedRate(()->LOGGER.debug("seen: {}",
    // seenBoards.stream().mapToInt(Map::size).sum()), 2, 5, TimeUnit.SECONDS);
  }

  private int processRecord(ByteBuffer state, ByteBuffer parent, int displayRow) {
    // Compute child moves and send to C*.
    // TODO: If a child is a rotational equivalent of another child, exclude one.

    // Check if this state is already stored in C*.
    ResultSet rs =
        session.execute(
            selectStatement.boundStatementBuilder().setByteBuffer("state", state).build());
    Row one = rs.one();
    if (one != null) {
      // It already exists in C*, but its parents list might not contain the given parent.
      // Add it if necessary.
      Set<ByteBuffer> persistedParents = one.getSet("parents", ByteBuffer.class);
      if (!persistedParents.contains(parent)) {
        addParent(state, parent);
      }
      return displayRow;
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
    sendToCassandra(stateBitSet, children, parent);

    // Send child tasks to Kafka, for any child that hasn't yet been explored. For those
    // that have been explored, add "current" as a parent of the child.
    // TODO: If a child is a rotational equivalent of another child (in this set of
    // children), skip it. If it's equivalent to a child in the db, update that child to include
    // this child's parent.
    if (children != null) {
      for (ByteBuffer child : children) {
        rs =
            session.execute(
                selectStatement.boundStatementBuilder().setByteBuffer("state", child).build());
        Row row = rs.one();
        if (row == null) {
          producer.send(
              new ProducerRecord<>(String.format("%s_%d", TOPIC_BASE, consumeLevel + 1), child,
                  state));
        } else {
          LOGGER.debug("Child {} already exists in db", BitSet.valueOf(child));
          displayRow++;
          addParent(child, state);
        }
      }
      producer.flush();
    }
    return displayRow;
  }

  private void readAndProcessTopic() {
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("key.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("value.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("group.id", "dumper2");
    consumerProps.put("client.id", CLIENT_ID);
    consumerProps.put("auto.offset.reset", "earliest");

    KafkaConsumer<ByteBuffer, ByteBuffer> dumpConsumer = new KafkaConsumer<>(consumerProps);

    // Subscribe to the topic.
    dumpConsumer.subscribe(Collections.singletonList(TOPIC_BASE));

    int numTasks = 0;
    int numUniqueTasks = 0;
    int numDupsWithNullParents = 0;
    int numDupsWithNonNullParents = 0;
    int numDiffParents = 0;
    while (true) {
      ConsumerRecords<ByteBuffer, ByteBuffer> tasks = dumpConsumer.poll(5000);
      if (tasks.isEmpty()) {
        System.out.println("No work!");
        break;
      }

      Map<ByteBuffer, ByteBuffer> seen = new HashMap<>();
      for (ConsumerRecord<ByteBuffer, ByteBuffer> task : tasks) {
        numTasks++;
        ByteBuffer state = task.key();
        ByteBuffer parent = null;
        if (task.value() != null) {
          parent = task.value();
        }

        if (seen.containsKey(state)) {
          ByteBuffer seenParent = seen.get(state);
          boolean sameParent =
              (parent == null && seenParent == null)
                  || (parent != null && parent.equals(seenParent))
                  || (seenParent != null && seenParent.equals(parent));

          if (sameParent) {
            if (parent == null) {
              numDupsWithNullParents++;
            } else {
              numDupsWithNonNullParents++;
            }
          } else {
            numDiffParents++;
          }
        } else {
          seen.put(state, parent);
          numUniqueTasks++;
        }
      }
    }
    System.out.println(
        String.format(
            "Total Kakfa records: %d\nUnique tasks: %d\nDups with null parents: %d\nDups with non-null parents: %d\nDups with diff parents: %d",
            numTasks,
            numUniqueTasks,
            numDupsWithNullParents,
            numDupsWithNonNullParents,
            numDiffParents));
  }

  public static void main(String[] args) throws UnknownHostException {
    if (args.length != 1) {
      System.err.println("Usage: solitaire-solver <client-id>");
    }
    Main main = new Main(args[0]);
    //    main.readAndProcessTopic();
    //    main.close();

    BitSet state = new BitSet(SLOTS);
    state.set(0, SLOTS);
    state.clear(18);

    //    main.readFromDb();
    //    return;

    // Get the ball rolling by pushing a task to Kafka (the initial state of the board)
    if ("ij1".equals(args[0])) {
      main.producer.send(
          new ProducerRecord<>(TOPIC_BASE + "_1", ByteBuffer.wrap(state.toByteArray()), null));
      main.consumeLevel = 1;
      main.producer.flush();
    }

    // Now consume "tasks" from Kafka, where each task involves the following:
    // * Calculate the list of possible next moves from the current board.
    // * Add a row in C* with the current board, its children, and its parent.
    // * For each child, add a task in Kafka.
    int numDups = 0;
    int processedCount = 0;
    //    for (int ctr = 0; ctr < 10; ++ctr) {
    while (processedCount < 1000) {
      // Subscribe to the topic.
      main.consumer.subscribe(String.format("%s_%d", TOPIC_BASE, main.consumeLevel));

      ConsumerRecords<ByteBuffer, ByteBuffer> tasks = main.consumer.poll(5000);
      if (tasks.isEmpty()) {
        System.out.printf("Completed level %d!%n", main.consumeLevel);
        main.consumeLevel++;
        if (main.consumeLevel == SLOTS) {
          // Completed last level. We're done!
          break;
        }
        continue;
      }
      for (ConsumerRecord<ByteBuffer, ByteBuffer> task : tasks) {
        ByteBuffer parent = null;
        if (task.value() != null) {
          parent = task.value();
        }
        numDups = main.processRecord(task.key(), parent, numDups);
        processedCount++;
      }
    }
    //
    ////
    ////    Board b = new Board(state, 0);
    ////
    ////    main.startConsumer();
    ////    main.readFromDb();
    ////    main.traverseChildren(b);
    //
    Runtime.getRuntime().addShutdownHook(new Thread(main::close));
    main.close();
    ////    SolitaireBoard solitaireBoard = new SolitaireBoard("Sol 0", state);
    ////    solitaireBoard.setVisible(true);
    ////    solitaireBoard.setLocation(0, 100);
    ////
    ////    for (Map<Board, Integer> boards : main.seenBoards) {
    ////      if (boards.size() == 1) {
    ////        Board board = boards.keySet().iterator().next();
    ////        solitaireBoard = new SolitaireBoard(String.format("Sol %d", board.getLevel()),
    // board.getState());
    ////        solitaireBoard.setVisible(true);
    ////        solitaireBoard.setLocation(board.getLevel() % 9 * 150, 100 + 200 * (board.getLevel()
    // / 9));
    //////      System.out.println(String.format("level %d: %d", level++, boards.size()));
    ////      }
    ////    }
  }

  private void close() {
    try {
      session.close();
      consumer.close();
    } catch (RuntimeException e) {
      // swallow
    }
  }

  //  private void startConsumer() {
  //    Thread consumerThread = new Thread(() -> {
  //
  //    });
  //    consumerThread.start();
  //  }

  private void renderBoardAncestry(ByteBuffer board, int level) {
    ResultSet rs =
        session.execute(
            selectStatement.boundStatementBuilder().setByteBuffer("state", board).build());
    Row one = rs.one();
    assert one != null;
    Set<ByteBuffer> persistedParents = one.getSet("parents", ByteBuffer.class);

    if (!persistedParents.isEmpty()) {
      renderBoardAncestry(persistedParents.iterator().next(), level + 1);
    }
    BitSet state = BitSet.valueOf(board);
    SolitaireBoard solitaireBoard = new SolitaireBoard(String.format("sol %d", level), state);
    solitaireBoard.setLocation(level % 9 * 150, 100 + 200 * (level / 9));
    solitaireBoard.setVisible(true);
  }

  private void readFromDb() {
    ResultSet result = session.execute("SELECT * FROM solitaire.boards");
    //    BitSet last = null;
    Map<BitSet, BitSet> seen = new HashMap<>();
    int ctr = 0;
    for (Row r : result.all()) {
      ByteBuffer stateBuffer = r.getByteBuffer("state");
      assert stateBuffer != null;
      BitSet state = BitSet.valueOf(stateBuffer);
      Set<ByteBuffer> parentBuffer = r.getSet("parents", ByteBuffer.class);
      BitSet parent = null;
      if (parentBuffer != null && !parentBuffer.isEmpty()) {
        parent = BitSet.valueOf(parentBuffer.iterator().next());
      }
      seen.put(state, parent);
      Set<ByteBuffer> children = r.getSet("children", ByteBuffer.class);
      //      if (children == null || children.isEmpty()) {
      //        last = state;
      //      }

      SolitaireBoard solitaireBoard = new SolitaireBoard("sol", state);
      solitaireBoard.setLocation(ctr % 9 * 150, 100 + 200 * (ctr / 9));
      solitaireBoard.setVisible(true);
      ctr++;
    }

    // Build boards from last to first and display them.
    //    BitSet current = last;
    //    SolitaireBoard solitaireBoard = new SolitaireBoard("Sol 0", current);
    //    solitaireBoard.setVisible(true);
    //    solitaireBoard.setLocation(0, 100);
    //    while (current != null) {
    //      SolitaireBoard solitaireBoard = new SolitaireBoard("sol", current);
    //      solitaireBoard.setLocation(ctr % 9 * 150, 100 + 200 * (ctr / 9));
    //      solitaireBoard.setVisible(true);
    //      ctr++;
    //      // Go to parent.
    //      current = seen.get(current);
    //    }
  }

  private void sendToCassandra(BitSet state, Set<ByteBuffer> childrenStates, ByteBuffer parent) {
    byte level = (byte) (SLOTS - state.cardinality());
    BoundStatementBuilder boundStatementBuilder = insertStatement.boundStatementBuilder();
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    boundStatementBuilder
        .setByteBuffer("state", stateBuffer)
        .setSet("children", childrenStates, ByteBuffer.class)
        .setString("client_id", clientId)
        .setByte("level", level);
    session.execute(boundStatementBuilder.build());
    addParent(stateBuffer, parent);

    // Update the metrics; run asynchronously and don't wait for the result. If there's a failure,
    // we don't really care.
    updateMetrics(level);
  }

  private void addParent(ByteBuffer stateBuffer, ByteBuffer parent) {
    // Add the parent; we do this separately from the INSERT above because multiple clients
    // may try to insert the same state at the same time (coming from different parents).
    // The concurrent inserts are idempotent (other than only one client getting final credit
    // for the state). By adding the parents afterward, via set addition, both/multiple parents
    // can be added atomically.
    BoundStatementBuilder boundStatementBuilder =
        updateParentsStatement
            .boundStatementBuilder()
            .setByteBuffer("state", stateBuffer)
            .setSet(
                "new_parent",
                parent == null ? null : Collections.singleton(parent),
                ByteBuffer.class);
    session.execute(boundStatementBuilder.build());
  }

  private void updateMetrics(byte level) {
    BoundStatementBuilder boundStatementBuilder;
    boundStatementBuilder =
        clientMetricsStatement.boundStatementBuilder().setString("client_id", clientId);
    session.executeAsync(boundStatementBuilder.build());

    boundStatementBuilder = levelMetricsStatement.boundStatementBuilder().setByte("level", level);
    session.executeAsync(boundStatementBuilder.build());
  }

  //  private void traverseChildren(Board b) {
  //    Set<ByteBuffer> parents = new HashSet<>();
  //    if (b.getParent() != null) {
  //      parents.add(ByteBuffer.wrap(b.getParent().getState().toByteArray()));
  //    }
  //    Set<ByteBuffer> childrenStates = new LinkedHashSet<>();
  //    Set<Board> children = new LinkedHashSet<>();
  //
  //    for (Board child : b) {
  //      children.add(child);
  //      childrenStates.add(ByteBuffer.wrap(child.getState().toByteArray()));
  //    }
  //
  //    sendToCassandra(b.getState(), childrenStates, parents);
  //
  //    //    for (Board child : children) {
  //    //      if (child.getLevel() == 29) {
  //    //        executorService.execute(() -> {
  //    //          seenBoards.get(b.getLevel()).put(child, 1);
  //    //          traverseChildren(child);
  //    //        });
  //    //      } else {
  //    //        seenBoards.get(b.getLevel()).put(child, 1);
  //    if (!children.isEmpty()) {
  //      traverseChildren(children.iterator().next());
  //    }
  //    // Uncomment if you don't want to explore children. FOR GRAPHING ONLY.
  //    //        break;
  //    //      }
  //  }
}
