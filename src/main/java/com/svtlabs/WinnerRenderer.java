package com.svtlabs;

import java.util.Collection;
import java.util.LinkedHashSet;

public class WinnerRenderer {
  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: WinnerRenderer <cassandra-seed>");
      System.exit(1);
    }
    CassandraClient cassandraClient = new CassandraClient(args[0]);
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
          nextRow.addAll(cassandraClient.getParents(b));
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
