package com.svtlabs;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

class MoveHelper {
  static List<List<Move>> MOVES = new ArrayList<>(Board.SLOTS);
  private static final int[] ROW_LENGTHS = {3, 5, 7, 7, 7, 5, 3, 99};

  // Map of state bits to rotate a board right by 90 degrees.
  // Use a simple array where the index represents the destination
  // bit index and the value is the source bit index.
  // Example: to say that bit 0 in the right-rotated board has
  // the value of bit 22 in the original board, ROTATE_RIGHT[0] = 22.
  private static final int[] ROTATE_RIGHT = {
    22, 15, 8, //
    29, 23, 16, 9, 3, //
    34, 30, 24, 17, 10, 4, 0, //
    35, 31, 25, 18, 11, 5, 1, //
    36, 32, 26, 19, 12, 6, 2, //
    33, 27, 20, 13, 7, //
    28, 21, 14
  };

  // Analogous to ROTATE_RIGHT, to flip a board along the vertical axis.
  private static final int[] MIRROR_ALONG_VERTICAL = {
    2, 1, 0, //
    7, 6, 5, 4, 3, //
    14, 13, 12, 11, 10, 9, 8, //
    21, 20, 19, 18, 17, 16, 15, //
    28, 27, 26, 25, 24, 23, 22, //
    33, 32, 31, 30, 29, //
    36, 35, 34
  };

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

  /** This is a helper that should never be instantiated. */
  private MoveHelper() {}

  static BitSet canonicalize(BitSet orig) {
    // Compute all equivalent boards by rotating and flipping this one.
    // The "canonical" board is defined to be the one with least value
    // when comparing bits.

    TreeSet<BitSet> equivalents = computeEquivalentStates(orig);
    equivalents.add(orig);
    return equivalents.first();
  }

  private static TreeSet<BitSet> computeEquivalentStates(BitSet orig) {
    // The board states that are equivalent to this one are
    // the states that are 90, 180, or 270 degrees rotated
    // from orig, as well as their mirror images.
    TreeSet<BitSet> result =
        new TreeSet<>(
            (lhs, rhs) -> {
              // From https://ideone.com/wwy1Nv
              if (lhs.equals(rhs)) {
                return 0;
              }
              BitSet xor = (BitSet) lhs.clone();
              xor.xor(rhs);
              int firstDifferent = xor.length() - 1;
              return rhs.get(firstDifferent) ? 1 : -1;
            });
    BitSet last = orig;
    result.add(transform(orig, MIRROR_ALONG_VERTICAL));
    for (int ctr = 0; ctr < 3; ctr++) {
      BitSet rotated = transform(last, ROTATE_RIGHT);
      result.add(rotated);
      result.add(transform(rotated, MIRROR_ALONG_VERTICAL));
      last = rotated;
    }
    return result;
  }

  private static BitSet transform(BitSet orig, int[] mapping) {
    BitSet result = new BitSet(mapping.length);
    for (int idx = 0; idx < mapping.length; idx++) {
      result.set(idx, orig.get(mapping[idx]));
    }
    return result;
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

  public static void main(String[] args) {
    BitSet state = new BitSet(Board.SLOTS);
    state.set(0, true);
    state.set(3, true);
    state.set(9, true);
    state.set(23, true);

    Visualization.renderBoards2(Collections.singletonList(state), 300, 100);
    Visualization.renderBoards2(MoveHelper.computeEquivalentStates(state), 300, 200);
  }
}
