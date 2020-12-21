package com.svtlabs;

import com.svtlabs.jedis.RedisClient;

public class InitialBoard {
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: InitialBoard <redis-host-port-or-pipe-path> <empty-slot-num>");
      System.exit(1);
    }

    int emptySlot = Integer.parseInt(args[1]);
    RedisClient redis = new RedisClient(args[0]);
    Board initial = Board.initial(emptySlot);
    redis.addTask(initial.getState());
    redis.close();
  }
}
