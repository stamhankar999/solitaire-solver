package com.svtlabs;

import java.util.Collections;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * KafkaConsumer wrapper class that keeps track of the subscribed topic and ignores subscribe
 * requests when the desired topic is already the currently subscribed topic.
 */
class ConsumerWithTopic<K, V> {
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

  @SuppressWarnings("SameParameterValue")
  @NotNull
  ConsumerRecords<K, V> poll(long timeout) {
    return wrapped.poll(timeout);
  }

  void close() {
    wrapped.close();
  }
}
