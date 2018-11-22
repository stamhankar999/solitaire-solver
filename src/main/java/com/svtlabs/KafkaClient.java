package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class KafkaClient {
  private static final String TOPIC_BASE = "solitaire";
  private static final String GROUP_ID = "Solitaire";
  private static final String CLIENT_ID = "Solitaire";
  private static final int POLL_TIMEOUT = 2000;
  @NotNull private final KafkaProducer<ByteBuffer, ByteBuffer> producer;
  @NotNull private final ConsumerWithTopic<ByteBuffer, ByteBuffer> consumer;

  KafkaClient() {
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
  }

  void addTask(int level, @NotNull ByteBuffer stateBytes, @Nullable ByteBuffer parent) {
    producer.send(new ProducerRecord<>(getTopicName(level), stateBytes, parent));
  }

  @NotNull
  Collection<BoardTask> consumeTasks(int level) {
    consumer.subscribe(getTopicName(level));
    ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
    List<BoardTask> result = new ArrayList<>(records.count());
    for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
      result.add(new BoardTask(record.key(), record.value()));
    }
    return result;
  }

  @NotNull
  private String getTopicName(int level) {
    return String.format("%s-%d", TOPIC_BASE, level);
  }

  void flush() {
    producer.flush();
  }

  void close() {
    consumer.close();
  }

  /** Debugging method - note that it hasn't been updated since we split topics. */
  @SuppressWarnings("unused")
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
}
