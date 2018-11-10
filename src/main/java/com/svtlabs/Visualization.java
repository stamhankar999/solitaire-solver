package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collection;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

@SuppressWarnings({"unused", "SameParameterValue", "WeakerAccess"})
class Visualization {
  /** This is a helper class, so don't allow instantiation. */
  private Visualization() {}

  static void renderBoardAncestry(
      @NotNull ByteBuffer board, int level, @NotNull CassandraClient cassandra) {
    PersistedBoard persistedBoard = cassandra.getPersistedBoard(board);
    assert persistedBoard != null;
    Set<ByteBuffer> persistedParents = persistedBoard.getParents();

    if (persistedParents != null && !persistedParents.isEmpty()) {
      renderBoardAncestry(persistedParents.iterator().next(), level + 1, cassandra);
    }
    BitSet state = BitSet.valueOf(board);
    RenderedBoard renderedBoard = new RenderedBoard(String.format("sol %d", level), state);
    renderedBoard.setLocation(level % 9 * 150, 100 + 200 * (level / 9));
    renderedBoard.setVisible(true);
  }

  static void renderBoards(@NotNull Collection<PersistedBoard> boards, int startX, int startY) {
    int idx = 0;
    final int numPerRow = 12;
    for (PersistedBoard board : boards) {
      RenderedBoard renderedBoard =
          new RenderedBoard(String.format("sol %d", idx), BitSet.valueOf(board.getState()));
      renderedBoard.setLocation(startX + idx % numPerRow * 150, startY + 150 * (idx / numPerRow));
      renderedBoard.setVisible(true);
      idx++;
    }
  }

  static void renderBoards2(@NotNull Collection<BitSet> boards, int startX, int startY) {
    int idx = 0;
    final int numPerRow = 12;
    for (BitSet board : boards) {
      RenderedBoard renderedBoard = new RenderedBoard(String.format("sol %d", idx), board);
      renderedBoard.setLocation(startX + idx % numPerRow * 150, startY + 150 * (idx / numPerRow));
      renderedBoard.setVisible(true);
      idx++;
    }
  }
}
