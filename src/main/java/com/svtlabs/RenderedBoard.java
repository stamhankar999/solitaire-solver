package com.svtlabs;

import java.awt.*;
import java.util.BitSet;
import javax.swing.*;
import org.jetbrains.annotations.NotNull;

public class RenderedBoard extends JFrame {
  private static final int SIZE = 10;
  @NotNull private final BitSet state;
  private static final int LEFT_MARGIN = 10;
  private static final int TOP_MARGIN = 35;

  RenderedBoard(@NotNull String title, @NotNull BitSet state) throws HeadlessException {
    this.state = state;

    setTitle(title);
    setSize(8 * SIZE, 8 * SIZE + 50);
    setDefaultCloseOperation(DISPOSE_ON_CLOSE);
  }

  @Override
  public void paint(@NotNull Graphics g) {
    super.paint(g);
    int pos = 0;
    for (int row = 0; row < 7; row++) {
      int colStart;
      int colEnd;
      if (row < 2) {
        colStart = 2 - row;
        colEnd = row + 4;
      } else if (row > 4) {
        colStart = row - 4;
        colEnd = 10 - row;
      } else {
        colStart = 0;
        colEnd = 6;
      }

      for (int col = colStart; col <= colEnd; ++col) {
        int x = col * SIZE + LEFT_MARGIN;
        int y = row * SIZE + TOP_MARGIN;
        //        g.drawString(String.format("%d", pos++),x, y);
        if (state.get(pos++)) {
          g.fillOval(x, y, SIZE, SIZE);
        } else {
          g.drawOval(x, y, SIZE, SIZE);
        }
      }
    }
  }

  public static void main(String args[]) {
    BitSet state = new BitSet(Board.SLOTS);
    state.set(0, 37);
    state.clear(18);
    RenderedBoard board = new RenderedBoard("yo", state);
    board.setVisible(true);
    board.setLocation(200, 200);
  }
}
