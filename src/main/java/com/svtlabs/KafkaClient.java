package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteBufferDeserializer;
import org.apache.kafka.common.serialization.ByteBufferSerializer;
import org.jetbrains.annotations.NotNull;

class KafkaClient {
  private static final String GROUP_ID = "Solitaire";
  private static final long POLL_TIMEOUT = 1000;
  @NotNull private final String topicName;
  @NotNull private final KafkaProducer<ByteBuffer, ByteBuffer> producer;
  @NotNull private final KafkaConsumer<ByteBuffer, ByteBuffer> consumer;

  KafkaClient(String clientId, String bootstrapServers, String topicName) {
    this.topicName = topicName;
    // Connect to Kafka and create our producer and consumer objects.
    Properties producerProps = new Properties();
    producerProps.put("client.id", clientId);
    producerProps.put("bootstrap.servers", bootstrapServers);
    producerProps.put("retries", "2");
    producerProps.put("acks", "all");
    producerProps.put("key.serializer", ByteBufferSerializer.class);
    producerProps.put("value.serializer", ByteBufferSerializer.class);
    producer = new KafkaProducer<>(producerProps);

    Properties consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", bootstrapServers);
    consumerProps.put("key.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("value.deserializer", ByteBufferDeserializer.class);
    consumerProps.put("group.id", GROUP_ID);
    consumerProps.put("client.id", clientId);
    consumerProps.put("auto.offset.reset", "earliest");
    consumerProps.put("max.poll.records", "1");

    // Create the consumer using props.
    consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singletonList(topicName));
  }

  void addTask(@NotNull ByteBuffer stateBytes) {
    producer.send(new ProducerRecord<>(topicName, stateBytes));
  }

  @NotNull
  Collection<BoardTask> consumeTasks() {
    ConsumerRecords<ByteBuffer, ByteBuffer> records = consumer.poll(POLL_TIMEOUT);
    List<BoardTask> result = new ArrayList<>(records.count());
    for (ConsumerRecord<ByteBuffer, ByteBuffer> record : records) {
      result.add(new BoardTask(record.value()));
    }
    return result;
  }

  void flush() {
    producer.flush();
  }

  void close() {
    producer.close();
    consumer.close();
  }

  public void commitAsync() {
    consumer.commitAsync();
  }
}
