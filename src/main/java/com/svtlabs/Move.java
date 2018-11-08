package com.svtlabs;

import org.jetbrains.annotations.NotNull;

class Move {
  private final int over;
  private final int to;

  Move(int over, int to) {
    this.over = over;
    this.to = to;
  }

  int getOver() {
    return over;
  }

  int getTo() {
    return to;
  }

  @Override
  @NotNull
  public String toString() {
    return String.format("%d,%d", over, to);
  }
}
