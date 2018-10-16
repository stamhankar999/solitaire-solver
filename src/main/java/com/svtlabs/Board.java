package com.svtlabs;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Board implements Iterable<Board> {
  private final BitSet state;
  private final int level;
  private final Board parent;

  public Board(BitSet state, int level) {
    this(state, level, null);
  }

  private Board(BitSet state, int level, Board parent)
  {
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

  public BitSet getState() {
    return state;
  }

  public int getLevel() {
    return level;
  }

  public Board getParent() {
    return parent;
  }

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
        moves = possibleMoves.stream().filter(m -> state.get(m.getOver()) && !state.get(m.getTo())).collect(Collectors.toList());
        if (!moves.isEmpty()) {
          moveIterator = moves.iterator();
          return true;
        }
      }
      return false;
    }

    @Override
    public Board next() {
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
