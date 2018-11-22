package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * Utility/test class that runs the traversal in memory upto N levels and compares the result to the
 * PersistedBoard's stored in C* in the last run of the application.
 *
 * <p>The idea is to somewhat independently verify that data is stored properly in C*.
 */
public class BoardValidator {
  private static final Map<ByteBuffer, PersistedBoard> seenBoards = new HashMap<>();

  private static void traverse(@NotNull Board b) {
    if (b.getLevel() == 8) {
      // Stop traversing.
      return;
    }
    ByteBuffer state = ByteBuffer.wrap(MoveHelper.canonicalize(b.getState()).toByteArray());
    PersistedBoard persistedBoard = seenBoards.get(state);
    if (persistedBoard != null) {
      // We've seen this board before. Add its newly discovered parent to the existing
      // PersistedBoard.
      Board parent = b.getParent();
      if (parent != null) {
        persistedBoard.addParent(
            ByteBuffer.wrap(MoveHelper.canonicalize(parent.getState()).toByteArray()));
      }
      return;
    }

    persistedBoard = new PersistedBoard(state, (byte) b.getLevel(), (byte) Board.SLOTS);
    seenBoards.put(state, persistedBoard);
    Board parent = b.getParent();
    if (parent != null) {
      persistedBoard.addParent(
          ByteBuffer.wrap(MoveHelper.canonicalize(parent.getState()).toByteArray()));
    }
    for (Board child : b) {
      persistedBoard.addChild(
          ByteBuffer.wrap(MoveHelper.canonicalize(child.getState()).toByteArray()));
      traverse(child);
    }
  }

  public static void main(String[] args) {
    CassandraClient client = new CassandraClient("test");

    // Run the algorithm and collect boards up through level 7.
    Board b = Board.initial();
    traverse(b);

    // Now retrieve all of the persistent boards from C*.

    @NotNull Set<PersistedBoard> actualBoards = client.getAllPersistedBoards();

    if (seenBoards.size() != actualBoards.size()) {
      System.err.println("Expected and actual boards collection size are different!");
      System.err.printf("  expected: %d\n  actual: %d\n", seenBoards.size(), actualBoards.size());
    }

    // Walk through actuals to make sure every PersistedBoard is equivalent to the one in
    // seenBoards

    List<PersistedBoard> mismatches = actualBoards.stream()
        .filter(actualPb -> !(seenBoards.get(actualPb.getState()).equals(actualPb)))
        .collect(Collectors.toList());
    if (mismatches.isEmpty()) {
      System.out.println("Hooray!");
    } else {
      System.err.printf("Found %d mismatches out of a total %d boards.\n", mismatches.size(), actualBoards.size());
    }
    client.close();
  }
}
