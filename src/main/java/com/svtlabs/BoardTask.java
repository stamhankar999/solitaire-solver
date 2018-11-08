package com.svtlabs;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class BoardTask {
  @NotNull private final ByteBuffer state;
  @Nullable private final ByteBuffer parent;

  BoardTask(@NotNull ByteBuffer state, @Nullable ByteBuffer parent) {
    this.state = state;
    this.parent = parent;
  }

  @NotNull
  ByteBuffer getState() {
    return state;
  }

  @Nullable
  ByteBuffer getParent() {
    return parent;
  }
}
