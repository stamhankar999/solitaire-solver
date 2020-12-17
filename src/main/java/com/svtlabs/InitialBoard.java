package com.svtlabs;

import com.svtlabs.jedis.RedisClient;

public class InitialBoard {
  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: InitialBoard <client-id> <empty-slot-num>");
      System.exit(1);
    }

    int emptySlot = Integer.parseInt(args[1]);
    RedisClient redis = new RedisClient("dummy", args[0]);
    Board initial = Board.initial(emptySlot);
    redis.addTask(initial.getState());
    redis.close();
  }
}
