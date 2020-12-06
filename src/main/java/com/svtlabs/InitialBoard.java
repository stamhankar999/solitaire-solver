package com.svtlabs;

import java.nio.ByteBuffer;

public class InitialBoard {
  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println(
          "Usage: InitialBoard <client-id> <bootstrap-server> <topic-name> <empty-slot-num>");
      System.exit(1);
    }

    int emptySlot = Integer.parseInt(args[3]);
    KafkaClient kafka = new KafkaClient(args[0], args[1], args[2]);
    Board initial = Board.initial(emptySlot);
    kafka.addTask(ByteBuffer.wrap(initial.getState().toByteArray()));
    kafka.close();
  }
}
