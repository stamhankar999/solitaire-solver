package com.svtlabs;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Board implements Iterable<Board> {
  static final int SLOTS = 37;
  @NotNull private final BitSet state;
  private final int level;
  @Nullable private final Board parent;

  /** Create a board with the initial state (e.g. a peg in every slot except for center). */
  static Board initial() {
    BitSet state = new BitSet(Board.SLOTS);
    state.set(0, Board.SLOTS);
    state.clear(18);

    return new Board(state, 1);
  }

  Board(@NotNull BitSet state, int level) {
    this(state, level, null);
  }

  private Board(@NotNull BitSet state, int level, @Nullable Board parent) {
    this.state = state;
    this.level = level;
    this.parent = parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Board board = (Board) o;
    return Objects.equals(state, board.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }

  @SuppressWarnings("NullableProblems")
  BitSet getState() {
    return state;
  }

  int getLevel() {
    return level;
  }

  @Nullable
  Board getParent() {
    return parent;
  }

  @NotNull
  @Override
  public Iterator<Board> iterator() {
    return new BoardIterator();
  }

  @Override
  public void forEach(Consumer<? super Board> action) {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public Spliterator<Board> spliterator() {
    throw new UnsupportedOperationException("not supported");
  }

  private class BoardIterator implements Iterator<Board> {
    private int pos;
    private List<Move> moves;
    private Iterator<Move> moveIterator;

    BoardIterator() {
      pos = -1;
      moves = new ArrayList<>();
      moveIterator = moves.iterator();
    }

    @Override
    public boolean hasNext() {
      if (moveIterator.hasNext()) {
        return true;
      }
      // No legal moves left for this pos. Try next pos.

      while (pos < 37) {
        // First, find the next pos that has a peg.
        pos = state.nextSetBit(++pos);
        if (pos < 0) {
          // No pegs left.
          return false;
        }

        List<Move> possibleMoves = MoveHelper.MOVES.get(pos);
        moves =
            possibleMoves
                .stream()
                .filter(m -> state.get(m.getOver()) && !state.get(m.getTo()))
                .collect(Collectors.toList());
        if (!moves.isEmpty()) {
          moveIterator = moves.iterator();
          return true;
        }
      }
      return false;
    }

    @Override
    public @Nullable Board next() {
      Move move = moveIterator.next();
      BitSet newState = (BitSet) state.clone();
      // Apply the move in newState:
      // 1. Clear pos, since that peg is moving.
      // 2. Clear the "over", since we're removing it.
      // 3. Set the "to", since that's the destination.
      newState.clear(pos);
      newState.clear(move.getOver());
      newState.set(move.getTo());
      return new Board(newState, level + 1, Board.this);
    }
  }
}
