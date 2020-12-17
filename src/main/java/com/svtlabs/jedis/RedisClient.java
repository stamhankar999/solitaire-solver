package com.svtlabs.jedis;

import static com.svtlabs.jedis.EncodingUtil.*;

import com.svtlabs.Board;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;

public class RedisClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
  private static byte[] PENDING;
  private static byte[] WORKING;
  private static byte[] COMPLETED;
  static {
    try {
      PENDING = "pending".getBytes(Protocol.CHARSET);
      WORKING = "working".getBytes(Protocol.CHARSET);
      COMPLETED = "completed".getBytes(Protocol.CHARSET);
    } catch (UnsupportedEncodingException e) {
      // This should never happen.
      e.printStackTrace();
    }
  }
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
    byte[] msg = jedis.brpoplpush(PENDING, WORKING, POLL_TIMEOUT);
    if (msg == null) {
      return null;
    }
    return new BoardTask(ByteBuffer.wrap(msg));
  }

  public boolean boardExists(ByteBuffer state) {
    return jedis.hexists(COMPLETED, state.array());
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

      try {
        pipeline.hset(COMPLETED, state.toByteArray(), clientId.getBytes(Protocol.CHARSET));
      } catch (UnsupportedEncodingException e) {
        // This should never happen.
        e.printStackTrace();
      }
      pipeline.lrem(WORKING, 1, state.toByteArray());
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
      pipeline.lpush(PENDING, child);
    }
  }
}
