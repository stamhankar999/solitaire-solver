package com.svtlabs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class CassandraClient {
  private final PreparedStatement insertBoardStatement;
  private final PreparedStatement insertWinningBoardStatement;
  private final PreparedStatement selectBoardStatement;
  private final PreparedStatement clientMetricsStatement;
  private final PreparedStatement levelMetricsStatement;
  private final PreparedStatement insertRelationStatement;
  private final PreparedStatement selectParentsStatement;

  private final CqlSession session;
  private final String clientId;

  CassandraClient(@NotNull String clientId, @NotNull String seed) {
    this.clientId = clientId;

    // Connect to C*.

    session = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(seed, 9042)).build();
    insertBoardStatement =
        session.prepare(
            "INSERT INTO solitaire.boards (state, client_id) VALUES (:state, :client_id)");
    insertWinningBoardStatement =
        session.prepare("INSERT INTO solitaire.winning_boards (state) VALUES (:state)");
    insertRelationStatement =
        session.prepare("INSERT INTO solitaire.board_rel (child, parent) VALUES (:child, :parent)");
    selectBoardStatement =
        session.prepare("SELECT state FROM solitaire.boards WHERE state = :state");
    selectParentsStatement =
        session.prepare("SELECT parent FROM solitaire.board_rel WHERE child = :child");
    clientMetricsStatement =
        session.prepare(
            "UPDATE solitaire.client_metrics SET boards_processed = boards_processed + 1 WHERE client_id = :client_id");
    levelMetricsStatement =
        session.prepare(
            "UPDATE solitaire.level_metrics SET boards_processed = boards_processed + 1 WHERE level = :level");
  }

  CompletionStage<? extends AsyncResultSet> addBoardRelation(ByteBuffer child, ByteBuffer parent) {
    BoundStatementBuilder boundStatementBuilder = insertRelationStatement.boundStatementBuilder();
    boundStatementBuilder.setByteBuffer("child", child).setByteBuffer("parent", parent);
    return session.executeAsync(boundStatementBuilder.build());
  }

  Collection<CompletionStage<? extends AsyncResultSet>> storeBoard(@NotNull BitSet state) {
    List<CompletionStage<? extends AsyncResultSet>> futures = new ArrayList<>();
    byte level = (byte) (Board.SLOTS - state.cardinality());
    BoundStatementBuilder boundStatementBuilder = insertBoardStatement.boundStatementBuilder();
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    boundStatementBuilder.setByteBuffer("state", stateBuffer).setString("client_id", clientId);
    futures.add(session.executeAsync(boundStatementBuilder.build()));

    boundStatementBuilder =
        clientMetricsStatement.boundStatementBuilder().setString("client_id", clientId);
    futures.add(session.executeAsync(boundStatementBuilder.build()));

    boundStatementBuilder = levelMetricsStatement.boundStatementBuilder().setByte("level", level);
    futures.add(session.executeAsync(boundStatementBuilder.build()));
    return futures;
  }

  CompletionStage<? extends AsyncResultSet> storeWinningBoard(@NotNull BitSet state) {
    BoundStatementBuilder boundStatementBuilder =
        insertWinningBoardStatement.boundStatementBuilder();
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    boundStatementBuilder.setByteBuffer("state", stateBuffer);
    return session.executeAsync(boundStatementBuilder.build());
  }

  @Nullable
  Board getBoard(@NotNull ByteBuffer state) {
    ResultSet rs =
        session.execute(
            selectBoardStatement.boundStatementBuilder().setByteBuffer("state", state).build());
    Row row = rs.one();
    if (row == null) {
      return null;
    }
    return rowToBoard(row, "state");
  }

  public Collection<Board> getWinningBoards() {
    ResultSet rs = session.execute("SELECT * FROM solitaire.winning_boards");
    List<Board> winningBoards = new ArrayList<>();
    for (Row row : rs) {
      winningBoards.add(rowToBoard(row, "state"));
    }
    return winningBoards;
  }

  public Collection<? extends Board> getParents(Board b) {
    ByteBuffer state = ByteBuffer.wrap(b.getState().toByteArray());
    ResultSet rs =
        session.execute(
            selectParentsStatement.boundStatementBuilder().setByteBuffer("child", state).build());
    List<Board> parents = new ArrayList<>();
    for (Row row : rs) {
      parents.add(rowToBoard(row, "parent"));
    }
    return parents;
  }

  //  /**
  //   * Get all persisted boards from the db and group them by level number. NOTE: level N boards
  // are
  //   * stored in index N-1.
  //   */
  //  @SuppressWarnings("unused")
  //  @NotNull
  //  List<Collection<Board>> getAllBoards() {
  //    ResultSet rs = session.execute("SELECT * FROM solitaire.boards");
  //    List<Collection<Board>> boards = new ArrayList<>(Board.SLOTS);
  //    for (int i = 0; i < Board.SLOTS; i++) {
  //      boards.add(null);
  //    }
  //
  //    for (Row row : rs) {
  //      Board board = rowToBoard(row);
  //      Collection<Board> coll = boards.get(board.getLevel() - 1);
  //      if (coll == null) {
  //        coll = new ArrayList<>();
  //        boards.set(board.getLevel() - 1, coll);
  //      }
  //      coll.add(board);
  //    }
  //    return boards;
  //  }

  @NotNull
  private Board rowToBoard(Row row, String fieldName) {
    ByteBuffer state = row.getByteBuffer(fieldName);
    assert state != null;
    return new Board(state);
  }

  void close() {
    session.close();
  }

  public static void main(String[] args) {
    CassandraClient client = new CassandraClient("test", "localhost");

    // Create a hierarchy of boards
    //       b1
    //   b2    b3
    // b6    b4  b5

    // NOTE: This is a good basis for an integration test, so don't delete this after
    // manual testing is completed.

    BitSet b1Bits = new BitSet(Board.SLOTS);
    b1Bits.set(0, Board.SLOTS);
    b1Bits.set(0, false);
    BitSet b2Bits = new BitSet(Board.SLOTS);
    b2Bits.set(1, true);
    BitSet b3Bits = new BitSet(Board.SLOTS);
    b3Bits.set(2, true);
    BitSet b4Bits = new BitSet(Board.SLOTS);
    b4Bits.set(3, true);
    BitSet b5Bits = new BitSet(Board.SLOTS);
    b5Bits.set(4, true);
    BitSet b6Bits = new BitSet(Board.SLOTS);
    b6Bits.set(5, true);

    ByteBuffer b2 = ByteBuffer.wrap(b2Bits.toByteArray());
    ByteBuffer b3 = ByteBuffer.wrap(b3Bits.toByteArray());

    client.storeBoard(b1Bits);
    client.addBoardRelation(b3, b2);

    client.close();
  }
}
