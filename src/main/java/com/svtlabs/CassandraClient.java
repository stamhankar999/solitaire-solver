package com.svtlabs;

import static com.svtlabs.Board.SLOTS;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class CassandraClient {
  private final PreparedStatement insertBoardStatement;
  private final PreparedStatement getBoardStatement;
  private final PreparedStatement clientMetricsStatement;
  private final PreparedStatement levelMetricsStatement;
  private final PreparedStatement insertParentStatement;
  private final PreparedStatement getParentsStatement;
  private final PreparedStatement updateBestResultStatement;

  private final CqlSession session;
  private final String clientId;

  CassandraClient(@NotNull String clientId) {
    this(clientId, "127.0.0.1");
  }

  CassandraClient(@NotNull String clientId, String host) {
    this.clientId = clientId;

    // Connect to C*.
    session = new CqlSessionBuilder()
        .addContactPoint(InetSocketAddress.createUnresolved(host, 9042))
        .build();
    insertBoardStatement =
        session.prepare(
            "INSERT INTO solitaire.boards "
                + "(state, children, client_id, level, best_result) "
                + "VALUES (:state, :children, :client_id, :level, "
                + Board.SLOTS
                + ")");
    insertParentStatement =
        session.prepare(
            "INSERT INTO solitaire.board_parents "
                + "(state, parent) "
                + "VALUES (:state, :parent)");
    updateBestResultStatement =
        session.prepare(
            "UPDATE solitaire.boards "
                + "SET best_result=:new_best WHERE state = :state "
                + "IF best_result = :cur_best");
    getBoardStatement = session.prepare("SELECT * FROM solitaire.boards WHERE state = :state");
    getParentsStatement = session.prepare("SELECT parent FROM solitaire.board_parents WHERE state = :state");
    clientMetricsStatement =
        session.prepare(
            "UPDATE solitaire.client_metrics "
                + "SET boards_processed = boards_processed + 1 "
                + "WHERE client_id = :client_id");
    levelMetricsStatement =
        session.prepare(
            "UPDATE solitaire.level_metrics "
                + "SET boards_processed = boards_processed + 1 "
                + "WHERE level = :level");
  }

  void storeBoard(
      @NotNull BitSet state,
      @Nullable Set<ByteBuffer> childrenStates) {
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    byte level = (byte) (Board.SLOTS - state.cardinality());

    BoundStatementBuilder boundStatementBuilder = insertBoardStatement.boundStatementBuilder();
    boundStatementBuilder
        .setByteBuffer("state", stateBuffer)
        .setSet("children", childrenStates, ByteBuffer.class)
        .setString("client_id", clientId)
        .setByte("level", level);
    session.execute(boundStatementBuilder.build());

    // Update the metrics; run asynchronously and don't wait for the result. If there's a failure,
    // we don't really care.
    updateMetrics(level);
  }

  private void addParent(@NotNull ByteBuffer stateBuffer, @NotNull ByteBuffer parent) {
    CompletionStage<? extends AsyncResultSet> stage = addParentAsync(stateBuffer, parent);
    stage.toCompletableFuture().join();
  }

  CompletionStage<? extends AsyncResultSet> addParentAsync(
      @NotNull ByteBuffer stateBuffer, @NotNull ByteBuffer parent) {
    BoundStatementBuilder boundStatementBuilder =
        insertParentStatement
            .boundStatementBuilder()
            .setByteBuffer("state", stateBuffer)
            .setByteBuffer("parent", parent);
    return session.executeAsync(boundStatementBuilder.build());
  }

  private void updateMetrics(byte level) {
    BoundStatementBuilder boundStatementBuilder;
    boundStatementBuilder =
        clientMetricsStatement.boundStatementBuilder().setString("client_id", clientId);
    session.executeAsync(boundStatementBuilder.build());

    boundStatementBuilder = levelMetricsStatement.boundStatementBuilder().setByte("level", level);
    session.executeAsync(boundStatementBuilder.build());
  }

  @Nullable
  PersistedBoard getPersistedBoard(@NotNull ByteBuffer state) {
    return getPersistedBoardAsync(state).toCompletableFuture().join();
  }

  CompletionStage<PersistedBoard> getPersistedBoardAsync(@NotNull ByteBuffer state) {
    CompletionStage<? extends AsyncResultSet> getBoardStage =
        session.executeAsync(
            getBoardStatement.boundStatementBuilder().setByteBuffer("state", state).build());
    CompletionStage<? extends AsyncResultSet> getParentsStage =
        session.executeAsync(
            getParentsStatement.boundStatementBuilder().setByteBuffer("state", state).build());

    return getBoardStage.thenCombine(getParentsStage, (boardRs, parentsRs) -> {
      Row boardRow = boardRs.one();
      if (boardRow == null) {
        return null;
      }

      // Walk through the parents result-set and collect all of the parents into a set.
      // TODO: Handle multiple pages.
      Set<ByteBuffer> parents = new HashSet<>();
      for (Row row : parentsRs.currentPage()) {
        parents.add(row.getByteBuffer(0));
      }
      return rowToPersistedBoard(boardRow, parents);
    });
  }

  /**
   * Get all persisted boards from the db and group them by level number. NOTE: level N boards are
   * stored in index N-1.
   */
  @NotNull
  List<Collection<PersistedBoard>> getAllPersistedBoardsByLevel() {
    ResultSet rs = session.execute("SELECT * FROM solitaire.boards");
    List<Collection<PersistedBoard>> boards = new ArrayList<>(SLOTS);
    for (int i = 0; i < SLOTS; i++) {
      boards.add(null);
    }

    for (Row row : rs) {
      ResultSet parentRs = session.execute(getParentsStatement.boundStatementBuilder().setByteBuffer("state", row.getByteBuffer("state")).build());
      Set<ByteBuffer> parents = new HashSet<>();
      for (Row parentRow : parentRs.all()) {
        parents.add(parentRow.getByteBuffer(0));
      }
      PersistedBoard board = rowToPersistedBoard(row, parents);
      Collection<PersistedBoard> coll = boards.get(board.getLevel() - 1);
      if (coll == null) {
        coll = new HashSet<>();
        boards.set(board.getLevel() - 1, coll);
      }
      coll.add(board);
    }
    return boards;
  }

  @NotNull
  Set<PersistedBoard> getAllPersistedBoards() {
    ResultSet rs = session.execute("SELECT * FROM solitaire.boards");
    Set<PersistedBoard> boards = new HashSet<>();
    for (Row row : rs) {
      ResultSet parentRs = session.execute(getParentsStatement.boundStatementBuilder().setByteBuffer("state", row.getByteBuffer("state")).build());
      Set<ByteBuffer> parents = new HashSet<>();
      for (Row parentRow : parentRs.all()) {
        parents.add(parentRow.getByteBuffer(0));
      }
      boards.add(rowToPersistedBoard(row, parents));
    }
    return boards;
  }

  @NotNull
  private PersistedBoard rowToPersistedBoard(Row row, @NotNull Set<ByteBuffer> parents) {
    Set<ByteBuffer> children = row.getSet("children", ByteBuffer.class);

    assert children != null;

    byte level = row.getByte("level");
    byte bestResult = row.getByte("best_result");
    ByteBuffer state = row.getByteBuffer("state");
    assert state != null;
    return new PersistedBoard(state, parents, children, level, bestResult);
  }

  void updateBestResult(@NotNull ByteBuffer stateBuf, byte newBest) {
    PersistedBoard pb = getPersistedBoard(stateBuf);
    assert pb != null;
    updateBestResult(pb, newBest);
  }

  private void updateBestResult(@NotNull PersistedBoard pb, byte newBest) {
    byte curBest = pb.getBestResult();
    ByteBuffer stateBuffer = pb.getState();

    boolean didUpdate = false;
    while (newBest < curBest) {
      // We've found a path to a better end result! Record it.
      // Others may concurrently update this board (because they may have found a better path as
      // well), so use an LWT to update the best_result atomically.

      BoundStatementBuilder boundStatementBuilder =
          updateBestResultStatement.boundStatementBuilder();
      boundStatementBuilder
          .setByteBuffer("state", stateBuffer)
          .setByte("cur_best", curBest)
          .setByte("new_best", newBest);

      ResultSet rs = session.execute(boundStatementBuilder.build());
      if (rs.wasApplied()) {
        didUpdate = true;
        break;
      } else {
        Row r = rs.one();
        assert r != null;
        Byte curBestRaw = r.get("best_result", GenericType.BYTE);
        assert curBestRaw != null;
        curBest = curBestRaw;
      }
    }

    // If we updated the best_result, propagate it to our parents.
    if (didUpdate) {
      Set<ByteBuffer> parents = pb.getParents();
      parents.forEach(p -> updateBestResult(p, newBest));
    }
  }

  void close() {
    session.close();
  }

  @SuppressWarnings({"ResultOfMethodCallIgnored", "unused"})
  public static void main(String[] args) throws IOException {
    CassandraClient client = new CassandraClient("test");

    // Create a hierarchy of boards
    //       b1
    //   b2    b3
    // b6    b4  b5

    // NOTE: This is a good basis for an integration test, so don't delete this after
    // manual testing is completed.

    BitSet b1Bits = new BitSet(Board.SLOTS);
    b1Bits.set(0, true);
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

    ByteBuffer b1 = ByteBuffer.wrap(b1Bits.toByteArray());
    ByteBuffer b2 = ByteBuffer.wrap(b2Bits.toByteArray());
    ByteBuffer b3 = ByteBuffer.wrap(b3Bits.toByteArray());
    ByteBuffer b4 = ByteBuffer.wrap(b4Bits.toByteArray());
    ByteBuffer b5 = ByteBuffer.wrap(b5Bits.toByteArray());
    ByteBuffer b6 = ByteBuffer.wrap(b6Bits.toByteArray());

    // TODO: This no longer synchronously updates parents.
    // Sync here before using this test code.
//    client.storeBoard(b1Bits, null, null);
//    client.storeBoard(b2Bits, null, b1);
//    client.storeBoard(b3Bits, null, b1);
//    client.storeBoard(b4Bits, null, b2);
//    client.addParent(b4, b3);
//    client.storeBoard(b5Bits, null, b3);
//    client.storeBoard(b6Bits, null, b2);

    // Now update b4
    client.updateBestResult(b4, (byte) 26);
    System.out.println(
        "b5, b6 should be 37. All others should have result 26. Hit enter to continue.");
    System.in.read();

    // Now update b5
    client.updateBestResult(b5, (byte) 20);
    System.out.println(
        "b1, b3, b5 should have result 20. b2, b4 should be 26. b6 should be 37. Hit enter to continue.");
    System.in.read();

    // Now update b6
    client.updateBestResult(b6, (byte) 22);
    System.out.println(
        "b1, b3, b5 should have result 20.  b2, b6 should have result 22. b4 should be 26.");

    client.close();
  }
}
