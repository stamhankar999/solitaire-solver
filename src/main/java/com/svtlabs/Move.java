package com.svtlabs;

public class Move {
  private final int over;
  private final int to;

  public Move(int over, int to) {
    this.over = over;
    this.to = to;
  }

  public int getOver() {
    return over;
  }

  public int getTo() {
    return to;
  }

  @Override
  public String toString() {
    return String.format("%d,%d", over, to);
  }
}
