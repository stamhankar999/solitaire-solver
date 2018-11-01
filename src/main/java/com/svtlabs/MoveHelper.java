package com.svtlabs;

import java.util.ArrayList;
import java.util.List;

public class MoveHelper {
  static List<List<Move>> MOVES = new ArrayList<>(37);
  private static int[] ROW_LENGTHS = {3, 5, 7, 7, 7, 5, 3, 99};

  static {
    // Populate MOVES
    for (int pos = 0; pos < 37; ++pos) {
      ArrayList<Move> cur = new ArrayList<>();
      MOVES.add(cur);

      int myrow = row(pos);

      // Right
      int over = pos + 1;
      int to = pos + 2;
      if (row(over) == myrow && row(to) == myrow) {
        cur.add(new Move(over, to));
      }

      // Left
      over = pos - 1;
      to = pos - 2;
      if (row(over) == myrow && row(to) == myrow) {
        cur.add(new Move(over, to));
      }

      // Down
      over = down(pos);
      to = down(over);
      if (row(over) == myrow + 1 && row(to) == myrow + 2) {
        cur.add(new Move(over, to));
      }

      // Up
      over = up(pos);
      to = up(over);
      if (row(over) == myrow - 1 && row(to) == myrow - 2) {
        cur.add(new Move(over, to));
      }
    }
  }

  private static int row(int pos) {
    if (pos < 0) {
      return -99;
    }
    if (pos < 3) {
      return 0;
    }
    if (pos < 8) {
      return 1;
    }
    if (pos < 15) {
      return 2;
    }
    if (pos < 22) {
      return 3;
    }
    if (pos < 29) {
      return 4;
    }
    if (pos < 34) {
      return 5;
    }
    if (pos < 37) {
      return 6;
    }
    return 99;
  }

  private static int down(int pos) {
    assert pos >= 0;
    if (pos < 8) {
      return pos + ROW_LENGTHS[row(pos) + 1] - 1;
    }
    if (pos < 22) {
      return pos + 7;
    }
    if (pos < 37) {
      return pos + ROW_LENGTHS[row(pos) + 1] + 1;
    }
    return 99;
  }

  private static int up(int pos) {
    if (pos < 4) {
      return -1;
    }
    if (pos < 15) {
      return pos - ROW_LENGTHS[row(pos) - 1] - 1;
    }
    if (pos < 29) {
      return pos - 7;
    }
    return pos - ROW_LENGTHS[row(pos) - 1] + 1;
  }
}
