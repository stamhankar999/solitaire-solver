package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

class PersistedBoard {
  @NotNull private final ByteBuffer state;
  @NotNull private final Set<ByteBuffer> parents;
  @NotNull private final Set<ByteBuffer> children;
  private final byte level;
  private final byte bestResult;

  PersistedBoard(
      @NotNull ByteBuffer state,
      @NotNull Set<ByteBuffer> parents,
      @NotNull Set<ByteBuffer> children,
      byte level,
      byte bestResult) {
    this.state = state;
    this.parents = parents;
    this.level = level;
    this.children = children;
    this.bestResult = bestResult;
  }

  @TestOnly
  PersistedBoard(@NotNull ByteBuffer state, byte level, byte bestResult) {
    this.state = state;
    this.level = level;
    this.bestResult = bestResult;
    this.children = new HashSet<>();
    this.parents = new HashSet<>();
  }

  @NotNull
  @SuppressWarnings("unused")
  ByteBuffer getState() {
    return state;
  }

  @NotNull
  Set<ByteBuffer> getParents() {
    return parents;
  }

  @SuppressWarnings("unused")
  @NotNull
  public Set<ByteBuffer> getChildren() {
    return children;
  }

  byte getLevel() {
    return level;
  }

  byte getBestResult() {
    return bestResult;
  }

  boolean containsParent(@Nullable ByteBuffer parent) {
    return parents.contains(parent);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PersistedBoard board = (PersistedBoard) o;

    boolean stateEquals = state.equals(board.state);
    boolean levelEquals = level == board.level;
    boolean bestResultEquals = bestResult == board.bestResult;
    boolean childrenEquals = children.equals(board.children);
    boolean parentEquals = parents.equals(board.parents);
    return stateEquals && levelEquals && bestResultEquals && childrenEquals && parentEquals;
  }

  @Override
  public int hashCode() {
    return Objects.hash(state, level, bestResult, children, parents);
  }

  @TestOnly
  void addParent(ByteBuffer parent) {
    parents.add(parent);
  }

  @TestOnly
  void addChild(ByteBuffer child) {
    children.add(child);
  }
}
