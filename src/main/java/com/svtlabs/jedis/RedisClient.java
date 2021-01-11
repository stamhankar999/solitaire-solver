package com.svtlabs.jedis;

import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.svtlabs.Board;
import com.svtlabs.Metrics;
import com.svtlabs.Util;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;

public class RedisClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);
  private static byte[] PENDING_TASKS;
  private static byte[] WORKING;
  private static byte[] COMPLETED;
  private static byte[] ERROR;
  private static byte[] PENDING_BOARDS;

  static {
    try {
      PENDING_TASKS = "pending".getBytes(Protocol.CHARSET);
      WORKING = "working".getBytes(Protocol.CHARSET);
      COMPLETED = "completed".getBytes(Protocol.CHARSET);
      ERROR = "error".getBytes(Protocol.CHARSET);
      PENDING_BOARDS = "pending_boards".getBytes(Protocol.CHARSET);
    } catch (UnsupportedEncodingException e) {
      // This should never happen.
      e.printStackTrace();
    }
  }

  private static final int POLL_TIMEOUT = 2;

  private final String clientId;
  private final UdsJedisFactory jedisFactory;
  private final Jedis jedis;
  private final Jedis jedis2;
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
      jedis2 = jedisFactory.create();
      errorJedis = jedisFactory.create();
    } else {
      // This must be a hostname / ip.
      jedisFactory = null;
      jedis = new Jedis(connectString);
      jedis2 = new Jedis(connectString);
      errorJedis = new Jedis(connectString);
    }
    this.maxBoardsInTask = maxBoardsInTask;
  }

  public void addTask(@NotNull ByteBuffer stateBytes) {
    BatchWriter batchWriter = createBatchWriter();
    batchWriter.addPotentialChildTaskFragment(stateBytes);
    batchWriter.execute(new Metrics(), null);
  }

  public void addTask(@NotNull BitSet stateBytes) {
    addTask(ByteBuffer.wrap(stateBytes.toByteArray()));
  }

  public synchronized void error(@NotNull BitSet stateBytes) {
    // This method is marked synchronized because multiple C* handler threads may
    // call this at the same time to record errors.
    errorJedis.rpush(ERROR, stateBytes.toByteArray());
  }

  public Map<Board, Boolean> checkCompletedBoards(Collection<Board> boards) {
    Map<Board, Response<Boolean>> results = new LinkedHashMap<>();
    Pipeline pipeline = jedis.pipelined();
    boards.forEach(b -> results.put(b, pipeline.hexists(COMPLETED, b.getState().toByteArray())));
    pipeline.sync();

    return Maps.transformValues(results, Response::get);
  }

  public void close() {
    jedis.close();
  }

  public ByteBuffer nextTask(List<Board> boards) {
    byte[] task = jedis.brpoplpush(PENDING_TASKS, WORKING, POLL_TIMEOUT);
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
    return new BatchWriter(clientId, jedis, jedis2, maxBoardsInTask);
  }

  public static class BatchWriter {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriter.class);
    private static final String CLIENT_STATS = "client_stats";
    private static final String LEVEL_STATS = "level_stats";
    private static final String ALREADY_SEEN_STATS = "already_seen_stats";
    private final Jedis jedis2;
    private final String clientId;
    private final Pipeline pipeline;
    private final Set<ByteBuffer> newBoards;
    private final int maxBoardsInTask;
    private final Map<Integer, Integer> levelStats;
    private final Map<Integer, Integer> alreadySeenStats;
    private int totalCompleted;

    public BatchWriter(String clientId, Jedis jedis, Jedis jedis2, int maxBoardsInTask) {
      this.jedis2 = jedis2;
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
          pipeline.srem(PENDING_BOARDS, boardState);
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
      Map<ByteBuffer, Boolean> foundPendingBoards =
          metrics.measure("redis.checkPendingBoards", () -> checkPendingBoards(newBoards));
      metrics.incrBy(
          "numExistingChildBoards", foundPendingBoards.values().stream().filter(b -> b).count());
      metrics.measure(
          "redis.prep",
          () -> {
            // Remove the task from the WORKING queue.
            if (rawTask != null) {
              pipeline.lrem(WORKING, 1, rawTask.array());
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "Candidate boards to investigate next:\n{}",
                  newBoards.stream()
                      .map(buf -> Util.bytesToHex(buf.array()))
                      .collect(Collectors.joining("\n")));
            }
            // Convert foundPendingBoards map to list of boards that aren't already pending.
            List<ByteBuffer> newPendingBoards =
                foundPendingBoards.entrySet().stream()
                    .filter(e -> !e.getValue())
                    .map(e -> e.getKey())
                    .collect(Collectors.toList());

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "foundPendingBoards mapping:\n{}",
                  foundPendingBoards.entrySet().stream()
                      .map(e -> Util.bytesToHex(e.getKey().array()) + ": " + e.getValue())
                      .collect(Collectors.joining("\n")));
            }

            if (LOGGER.isDebugEnabled()) {
              LOGGER.debug(
                  "Filtered boards to investigate next:\n{}",
                  newPendingBoards.stream()
                      .map(buf -> Util.bytesToHex(buf.array()))
                      .collect(Collectors.joining("\n")));
            }

            // Coalesce the new boards into groups of X to create multi-board task(s).
            Iterable<List<ByteBuffer>> groups =
                Iterables.partition(newPendingBoards, maxBoardsInTask);
            List<byte[]> newTasks = new ArrayList<byte[]>();
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
              newTasks.add(task.array());
            }
            if (!newTasks.isEmpty()) {
              pipeline.lpush(PENDING_TASKS, newTasks.toArray(new byte[1][]));
            }

            // Add the new boards to the pending-boards set.
            if (!newPendingBoards.isEmpty()) {
              List<byte[]> newPendingBoardsArray =
                  newPendingBoards.stream().map(ByteBuffer::array).collect(Collectors.toList());
              pipeline.sadd(PENDING_BOARDS, newPendingBoardsArray.toArray(new byte[1][]));
            }

            // Update stats.
            levelStats.forEach(
                (level, count) -> pipeline.hincrBy(LEVEL_STATS, String.valueOf(level), count));
            alreadySeenStats.forEach(
                (level, count) ->
                    pipeline.hincrBy(ALREADY_SEEN_STATS, String.valueOf(level), count));
            pipeline.hincrBy(CLIENT_STATS, clientId, totalCompleted);
          });

      metrics.measure("redis.update", () -> pipeline.sync());
    }

    public void addPotentialChildTaskFragment(ByteBuffer child) {
      newBoards.add(child);
    }

    private Map<ByteBuffer, Boolean> checkPendingBoards(Collection<ByteBuffer> boards) {
      Map<ByteBuffer, Response<Boolean>> results = new LinkedHashMap<>();
      Pipeline pipeline = jedis2.pipelined();
      boards.forEach(b -> results.put(b, pipeline.sismember(PENDING_BOARDS, b.array())));
      pipeline.sync();

      return Maps.transformValues(results, Response::get);
    }
  }
}
