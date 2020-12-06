package com.svtlabs;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

class BoardTask {
  @NotNull private final ByteBuffer state;

  BoardTask(@NotNull ByteBuffer state) {
    this.state = state;
  }

  @NotNull
  ByteBuffer getState() {
    return state;
  }
}
