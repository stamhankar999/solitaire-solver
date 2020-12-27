package com.svtlabs.jedis;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.svtlabs.Board;
import com.svtlabs.Metrics;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;

public class RedisClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
  private static byte[] PENDING;
  private static byte[] WORKING;
  private static byte[] COMPLETED;
  private static byte[] ERROR;

  static {
    try {
      PENDING = "pending".getBytes(Protocol.CHARSET);
      WORKING = "working".getBytes(Protocol.CHARSET);
      COMPLETED = "completed".getBytes(Protocol.CHARSET);
      ERROR = "error".getBytes(Protocol.CHARSET);
    } catch (UnsupportedEncodingException e) {
      // This should never happen.
      e.printStackTrace();
    }
  }

  private static final int POLL_TIMEOUT = 2;

  private final String clientId;
  private final UdsJedisFactory jedisFactory;
  private final Jedis jedis;
  private final Jedis errorJedis;
  private final int maxBoardsInTask;

  /** This constructor is only used in small programs that create an initial task. */
  public RedisClient(String connectString) {
    this("dummy", connectString, 10);
  }

  public RedisClient(String clientId, String connectString, int maxBoardsInTask) {
    this.clientId = clientId;
    if (connectString.contains("/")) {
      // This is a pipe path.
      jedisFactory = new UdsJedisFactory(connectString);
      jedis = jedisFactory.create();
      errorJedis = jedisFactory.create();
    } else {
      // This must be a hostname / ip.
      jedisFactory = null;
      jedis = new Jedis(connectString);
      errorJedis = new Jedis(connectString);
    }
    this.maxBoardsInTask = maxBoardsInTask;
  }

  public void addTask(@NotNull ByteBuffer stateBytes) {
    BatchWriter batchWriter = createBatchWriter();
    batchWriter.addTask(stateBytes);
    batchWriter.execute(new Metrics(), null);
  }

  public void addTask(@NotNull BitSet stateBytes) {
    addTask(ByteBuffer.wrap(stateBytes.toByteArray()));
  }

  public synchronized void error(@NotNull BitSet stateBytes) {
    // This method is marked synchronized because multiple C* handler threads may call this at
    // the same time to record errors.
    errorJedis.rpush(ERROR, stateBytes.toByteArray());
  }

  public Map<Board, Boolean> checkExistence(List<Board> task) {
    Map<Board, Response<Boolean>> results = new LinkedHashMap<>();
    Pipeline pipeline = jedis.pipelined();
    task.forEach(b -> results.put(b, pipeline.hexists(COMPLETED, b.getState().toByteArray())));
    pipeline.sync();

    return Maps.transformValues(results, Response::get);
  }

  public void close() {
    jedis.close();
  }

  public ByteBuffer nextTask(List<Board> boards) {
    byte[] task = jedis.brpoplpush(PENDING, WORKING, POLL_TIMEOUT);
    if (task == null) {
      return null;
    }

    ByteBuffer taskBuffer = ByteBuffer.wrap(task);
    while (taskBuffer.hasRemaining()) {
      byte boardLen = taskBuffer.get();
      byte[] board = new byte[boardLen];
      taskBuffer.get(board);
      boards.add(new Board(BitSet.valueOf(board)));
    }

    taskBuffer.rewind();
    return taskBuffer;
  }

  public boolean boardExists(ByteBuffer state) {
    return jedis.hexists(COMPLETED, state.array());
  }

  public BatchWriter createBatchWriter() {
    return new BatchWriter(clientId, jedis, maxBoardsInTask);
  }

  public static class BatchWriter {
    private static final String CLIENT_STATS = "client_stats";
    private static final String LEVEL_STATS = "level_stats";
    private static final String ALREADY_SEEN_STATS = "already_seen_stats";
    private final String clientId;
    private final Pipeline pipeline;
    private final Set<ByteBuffer> newBoards;
    private final int maxBoardsInTask;
    private final Map<Integer, Integer> levelStats;
    private final Map<Integer, Integer> alreadySeenStats;
    private int totalCompleted;

    public BatchWriter(String clientId, Jedis jedis, int maxBoardsInTask) {
      levelStats = new HashMap<>();
      alreadySeenStats = new HashMap<>();
      this.clientId = clientId;
      this.pipeline = jedis.pipelined();
      newBoards = new LinkedHashSet<>();
      this.maxBoardsInTask = maxBoardsInTask;
      totalCompleted = 0;
    }

    public void completed(Board board, boolean alreadyCompleted) {
      byte[] boardState = board.getState().toByteArray();
      if (alreadyCompleted) {
        int stat = alreadySeenStats.getOrDefault(board.getLevel(), 0);
        alreadySeenStats.put(board.getLevel(), stat + 1);
      } else {
        try {
          pipeline.hset(COMPLETED, boardState, clientId.getBytes(Protocol.CHARSET));
        } catch (UnsupportedEncodingException e) {
          // This should never happen.
          e.printStackTrace();
        }
      }

      int stat = levelStats.getOrDefault(board.getLevel(), 0);
      levelStats.put(board.getLevel(), stat + 1);
      totalCompleted++;
    }

    public void execute(Metrics metrics, ByteBuffer rawTask) {
      metrics.measure(
          "redis.prep",
          () -> {
            // Remove the task from the WORKING queue.
            if (rawTask != null) {
              pipeline.lrem(WORKING, 1, rawTask.array());
            }

            // Coalesce the new boards into groups of X to create multi-board task(s).
            Iterable<List<ByteBuffer>> groups = Iterables.partition(newBoards, maxBoardsInTask);
            for (List<ByteBuffer> chunk : groups) {
              // Walk through the chunk to get the byte lengths of each buffer, to find out
              // how many
              // bytes we need in our final task (of the form size,chunk,size,chunk,...).
              int totalBytes =
                  chunk.stream()
                      .mapToInt(
                          buffer -> {
                            return buffer.array().length + 1;
                          })
                      .sum();
              ByteBuffer task = ByteBuffer.allocate(totalBytes);
              chunk.forEach(
                  buffer -> {
                    task.put((byte) buffer.array().length);
                    task.put(buffer);
                  });
              pipeline.lpush(PENDING, task.array());
            }
            levelStats.forEach(
                (level, count) -> pipeline.hincrBy(LEVEL_STATS, String.valueOf(level), count));
            alreadySeenStats.forEach(
                (level, count) ->
                    pipeline.hincrBy(ALREADY_SEEN_STATS, String.valueOf(level), count));
            pipeline.hincrBy(CLIENT_STATS, clientId, totalCompleted);
          });

      metrics.measure("redis.update", () -> pipeline.sync());
    }

    public void addTask(ByteBuffer child) {
      newBoards.add(child);
    }
  }
}
