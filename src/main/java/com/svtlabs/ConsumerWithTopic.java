package com.svtlabs;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;

/**
 * KafkaConsumer wrapper class that keeps track of the subscribed topic and ignores subscribe
 * requests when the desired topic is already the currently subscribed topic.
 */
public class ConsumerWithTopic<K, V> {
  private final KafkaConsumer<K, V> wrapped;
  private String currentTopic;

  public ConsumerWithTopic(KafkaConsumer<K, V> wrapped) {
    this.wrapped = wrapped;
  }

  public void subscribe(@NotNull String topic) {
    if (topic.equals(currentTopic)) {
      // Not changing topics.
      return;
    }
    wrapped.subscribe(Collections.singletonList(topic));
  }

  public ConsumerRecords<K, V> poll(long timeout) {
    return wrapped.poll(timeout);
  }

  public void close() {
    wrapped.close();
  }
}
