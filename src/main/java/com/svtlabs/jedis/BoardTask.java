package com.svtlabs.jedis;

import java.nio.ByteBuffer;
import org.jetbrains.annotations.NotNull;

public class BoardTask {
  @NotNull private final ByteBuffer state;

  public BoardTask(@NotNull ByteBuffer state) {
    this.state = state;
  }

  @NotNull
  public ByteBuffer getState() {
    return state;
  }
}
