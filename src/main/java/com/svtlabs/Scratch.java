package com.svtlabs;

import com.svtlabs.jedis.RedisClient;

import java.util.BitSet;

public class Scratch {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: Scratch <redis-host-port-or-pipe-path>");
      System.exit(1);
    }

    RedisClient redis = new RedisClient(args[0]);

    BitSet state = new BitSet(Board.SLOTS);
    state.set(26);
    state.set(27);
    state.set(32);
    state.set(33);

    redis.addTask(state);
    redis.close();
  }
}
