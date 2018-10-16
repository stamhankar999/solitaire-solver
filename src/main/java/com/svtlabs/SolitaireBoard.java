package com.svtlabs;

import java.awt.*;
import java.util.BitSet;
import javax.swing.*;

public class SolitaireBoard extends JFrame
{
  static int SIZE = 22;
  private final BitSet state;

  public SolitaireBoard(String title, BitSet state) throws HeadlessException {
    this.state = state;

    setTitle(title);
    setSize(180, 220);
    setDefaultCloseOperation(DISPOSE_ON_CLOSE);
  }

  @Override public void paint(Graphics g)
  {
    super.paint(g);
    int pos = 0;
    for (int row = 0;  row < 7;  row++ ) {
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
        int x = col * SIZE;
        int y = (row + 2) * SIZE;
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
    BitSet state = new BitSet(37);
    state.set(0, 37);
    state.clear(18);
    SolitaireBoard board = new SolitaireBoard("yo", state);
    board.setVisible(true);
    board.setLocation(200, 200);
  }
}