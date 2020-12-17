package com.svtlabs;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;

public class RedisPlay {
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  private static JedisPoolConfig buildPoolConfig() {
    final JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(16);
    poolConfig.setMaxIdle(16);
    poolConfig.setMinIdle(1);
    poolConfig.setTestOnBorrow(true);
    poolConfig.setTestOnReturn(true);
    poolConfig.setTestWhileIdle(true);
    poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
    poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
    poolConfig.setNumTestsPerEvictionRun(3);
    poolConfig.setBlockWhenExhausted(true);
    return poolConfig;
  }

  public static void main(String[] args) {
    // Jedis jedis = new Jedis("192.168.86.51");
    Jedis jedis = new Jedis("localhost");
    // final JedisPoolConfig poolConfig = buildPoolConfig();
    // JedisPool jedisPool = new JedisPool(poolConfig, "localhost");
    // Jedis jedis = jedisPool.getResource();

    // UdsJedisFactory jedisFactory = new UdsJedisFactory("/tmp/redis.sock");
    // Jedis jedis = jedisFactory.create();
    Board initial = Board.initial(18);
    String boardAsString = new String(initial.getState().toByteArray());
    jedis.set(boardAsString, "rads-mac");
    System.out.println("New value: " + jedis.get(boardAsString));

    List<String> keys = new ArrayList<>();
    long start = System.currentTimeMillis();
    byte[] key = new byte[] {0, 0, 0, 0};
    int a, b = 0;
    for (a = 0; a < 200; a++) {
      for (b = 0; b < 50; b++) {
        for (byte c = 0; c < 1; c++) {
          key[0] = (byte) a;
          key[1] = (byte) b;
          key[2] = c;
          String stringKey = new String(key);
          keys.add(stringKey);
          jedis.set(stringKey, "rads-mac");
        }
      }
    }
    long end = System.currentTimeMillis();
    System.out.println("Total time for sets: " + (end - start) + " for " + a + "," + b);

    for (String k : keys) {
      jedis.get(k);
    }
    long readEnd = System.currentTimeMillis();
    System.out.println("Total time for gets: " + (readEnd - end) + " for " + keys.size() + " keys");

    for (String k : keys) {
      jedis.get(k);
    }
    long readEnd2 = System.currentTimeMillis();
    System.out.println(
        "Total time for gets again: " + (readEnd2 - readEnd) + " for " + keys.size() + " keys");
  }
}
