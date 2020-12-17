package com.svtlabs.jedis;

import static com.svtlabs.jedis.EncodingUtil.*;

import com.svtlabs.Board;
import java.nio.ByteBuffer;
import java.util.BitSet;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

public class RedisClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
  private static final String PENDING = "pending";
  private static final String WORKING = "working";
  private static final String COMPLETED = "completed";
  private static final int POLL_TIMEOUT = 2;

  private final String clientId;
  private final UdsJedisFactory jedisFactory;
  private final Jedis jedis;

  public RedisClient(String clientId, String pipePath) {
    this.clientId = clientId;
    jedisFactory = new UdsJedisFactory(pipePath);
    jedis = jedisFactory.create();
  }

  public void addTask(@NotNull ByteBuffer stateBytes) {
    addTask(stateBytes.array());
  }

  public void addTask(@NotNull BitSet stateBytes) {
    addTask(stateBytes.toByteArray());
  }

  public void addTask(@NotNull byte[] stateBytes) {
    BatchWriter batchWriter = createBatchWriter();
    batchWriter.addTask(stateBytes);
    batchWriter.execute();
  }

  public void close() {
    jedis.close();
  }

  public BoardTask nextTask() {
    String msg = jedis.brpoplpush(PENDING, WORKING, POLL_TIMEOUT);
    if (msg == null) {
      return null;
    }
    return new BoardTask(decodeToByteBuffer(msg));
  }

  public boolean boardExists(ByteBuffer state) {
    String msg = encode(state);
    return jedis.hexists(COMPLETED, msg);
  }

  public BatchWriter createBatchWriter() {
    return new BatchWriter(clientId, jedis);
  }

  public static class BatchWriter {
    private static final String CLIENT_STATS = "client_stats";
    private static final String LEVEL_STATS = "level_stats";
    private final String clientId;
    private final Pipeline pipeline;

    public BatchWriter(String clientId, Jedis jedis) {
      this.clientId = clientId;
      this.pipeline = jedis.pipelined();
    }

    public void completed(BitSet state) {
      int level = Board.SLOTS - state.cardinality();
      String value = encode(state);
      pipeline.hset(COMPLETED, value, clientId);
      pipeline.lrem(WORKING, 1, value);
      pipeline.hincrBy(LEVEL_STATS, String.valueOf(level), 1);
      pipeline.hincrBy(CLIENT_STATS, clientId, 1);
    }

    public void execute() {
      pipeline.sync();
    }

    public void addTask(ByteBuffer child) {
      addTask(child.array());
    }

    public void addTask(byte[] child) {
      String task = encode(child);
      pipeline.lpush(PENDING, task);
    }
  }
}
