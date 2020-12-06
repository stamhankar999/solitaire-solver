package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Scratch {
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: Scratch <client-id> <bootstrap-server> <topic-name>");
      System.exit(1);
    }

    KafkaClient kafka = new KafkaClient(args[0], args[1], args[2]);
    BitSet state = new BitSet(Board.SLOTS);
    state.set(26);
    state.set(27);
    state.set(32);
    state.set(33);

    kafka.addTask(ByteBuffer.wrap(state.toByteArray()));
    kafka.close();
  }

  @SuppressWarnings("unused")
  public static void stringConsume(String[] args) {
    if (args.length != 1) {
      System.err.println("Missing client-id arg");
    }
    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", "localhost:9092");
    consumerProps.put("key.deserializer", StringDeserializer.class);
    consumerProps.put("value.deserializer", StringDeserializer.class);
    consumerProps.put("group.id", "Scr7");
    consumerProps.put("client.id", args[0]);
    consumerProps.put("auto.offset.reset", "earliest");
    consumerProps.put("max.poll.records", "1");

    // Create the consumer using props.
    ConsumerWithTopic<String, String> consumer =
        new ConsumerWithTopic<>(new KafkaConsumer<>(consumerProps));
    consumer.subscribe("tp3");
    ConsumerRecords<String, String> records;
    long start = System.currentTimeMillis();
    Set<String> values = new TreeSet<>();
    while (true) {
      int pollCount = 0;
      do {
        ++pollCount;
        records = consumer.poll();
      } while (records.count() == 0 && pollCount < 5);
      if (records.count() == 0) {
        // The task queue is empty for a long time; we're done.
        break;
      }
      for (ConsumerRecord<String, String> record : records) {
        //        System.out.println("" + (System.currentTimeMillis() - start) + "\tkey: " +
        // record.key() + "  value: " + record.value());
        values.add(record.value());
      }
      //      try {
      //        Thread.sleep(200);
      //      } catch (InterruptedException e) {
      //        e.printStackTrace();
      //      }
      consumer.commitAsync();
    }
    System.out.println("Total time: " + (System.currentTimeMillis() - start));
    System.out.println(values);
  }

  /**
   * KafkaConsumer wrapper class that keeps track of the subscribed topic and ignores subscribe
   * requests when the desired topic is already the currently subscribed topic.
   */
  @SuppressWarnings({"SameParameterValue", "unused"})
  static class ConsumerWithTopic<K, V> {
    @NotNull private final KafkaConsumer<K, V> wrapped;
    @Nullable private String currentTopic;

    ConsumerWithTopic(@NotNull KafkaConsumer<K, V> wrapped) {
      this.wrapped = wrapped;
    }

    void subscribe(@NotNull String topic) {
      if (topic.equals(currentTopic)) {
        // Not changing topics.
        return;
      }
      currentTopic = topic;
      wrapped.subscribe(Collections.singletonList(topic));
    }

    @NotNull
    ConsumerRecords<K, V> poll() {
      return wrapped.poll(1000);
    }

    void close() {
      wrapped.close();
    }

    void commitAsync() {
      wrapped.commitAsync();
    }
  }
}
