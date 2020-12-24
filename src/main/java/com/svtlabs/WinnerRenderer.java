package com.svtlabs;

import com.svtlabs.jedis.RedisClient;
import java.util.Collection;
import java.util.LinkedHashSet;

public class WinnerRenderer {
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Usage: WinnerRenderer <client-id> <redis-target> <cassandra-seed>");
      System.exit(1);
    }
    CassandraClient cassandraClient = new CassandraClient(args[0], args[2]);
    RedisClient redis = new RedisClient(args[1]);

    Collection<Board> winners = cassandraClient.getWinningBoards();

    int posY = 100;
    for (Board winner : winners) {
      Collection<Board> row = new LinkedHashSet<>();
      Collection<Board> nextRow = new LinkedHashSet<>();
      row.add(winner);
      while (!row.isEmpty()) {
        Visualization.renderBoards(row, 100, posY);
        nextRow.clear();
        for (Board b : row) {
          nextRow.addAll(redis.getParents(b));
        }

        // Swap nextRow and row.
        Collection<Board> temp = row;
        row = nextRow;
        nextRow = temp;
        posY += 200;
      }
    }
  }
}
