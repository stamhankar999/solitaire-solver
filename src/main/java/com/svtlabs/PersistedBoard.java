package com.svtlabs;

import java.nio.ByteBuffer;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class PersistedBoard {
  @NotNull private final ByteBuffer state;
  @Nullable private final Set<ByteBuffer> parents;
  @Nullable private final Set<ByteBuffer> children;
  private final byte level;
  private final byte bestResult;

  PersistedBoard(
      @NotNull ByteBuffer state,
      @Nullable Set<ByteBuffer> parents,
      @Nullable Set<ByteBuffer> children,
      byte level,
      byte bestResult) {
    this.state = state;
    this.parents = parents;
    this.level = level;
    this.children = children;
    this.bestResult = bestResult;
  }

  @NotNull
  @SuppressWarnings("unused")
  ByteBuffer getState() {
    return state;
  }

  @Nullable
  Set<ByteBuffer> getParents() {
    return parents;
  }

  @SuppressWarnings("unused")
  @Nullable
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
    return parents != null && parents.contains(parent);
  }
}
