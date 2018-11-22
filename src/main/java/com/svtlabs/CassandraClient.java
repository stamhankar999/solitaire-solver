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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class CassandraClient {
  private final PreparedStatement insertStatement;
  private final PreparedStatement selectStatement;
  private final PreparedStatement clientMetricsStatement;
  private final PreparedStatement levelMetricsStatement;
  private final PreparedStatement updateParentsStatement;
  private final PreparedStatement updateBestResultStatement;

  private final CqlSession session;
  private final String clientId;

  CassandraClient(@NotNull String clientId) {
    this.clientId = clientId;

    // Connect to C*.
    session = new CqlSessionBuilder().build();
    insertStatement =
        session.prepare(
            "INSERT INTO solitaire.boards (state, children, client_id, level, best_result) VALUES (:state, :children, :client_id, :level, "
                + Board.SLOTS
                + ")");
    updateParentsStatement =
        session.prepare(
            "UPDATE solitaire.boards SET parents = parents + :new_parent WHERE state = :state IF parents = :cur_parents");
    updateBestResultStatement =
        session.prepare(
            "UPDATE solitaire.boards SET best_result=:new_best WHERE state = :state IF best_result = :cur_best");
    selectStatement = session.prepare("SELECT * FROM solitaire.boards WHERE state = :state");
    clientMetricsStatement =
        session.prepare(
            "UPDATE solitaire.client_metrics SET boards_processed = boards_processed + 1 WHERE client_id = :client_id");
    levelMetricsStatement =
        session.prepare(
            "UPDATE solitaire.level_metrics SET boards_processed = boards_processed + 1 WHERE level = :level");
  }

  void storeBoard(
      @NotNull BitSet state,
      @Nullable Set<ByteBuffer> childrenStates,
      @Nullable ByteBuffer parent) {
    byte level = (byte) (Board.SLOTS - state.cardinality());
    BoundStatementBuilder boundStatementBuilder = insertStatement.boundStatementBuilder();
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    boundStatementBuilder
        .setByteBuffer("state", stateBuffer)
        .setSet("children", childrenStates, ByteBuffer.class)
        .setString("client_id", clientId)
        .setByte("level", level);
    session.execute(boundStatementBuilder.build());
    if (parent != null) {
      addParent(stateBuffer, parent);
    }
    // Update the metrics; run asynchronously and don't wait for the result. If there's a failure,
    // we don't really care.
    updateMetrics(level);
  }

  void addParent(@NotNull ByteBuffer stateBuffer, @NotNull ByteBuffer parent) {
    PersistedBoard pb = getPersistedBoard(stateBuffer);
    assert pb != null;
    Set<ByteBuffer> currentParents = pb.getParents();

    while (currentParents == null || !currentParents.contains(parent)) {
//      Set<ByteBuffer> newParents = new HashSet<>();
//      if (currentParents != null) {
//        newParents.addAll(currentParents);
//      }
//      newParents.add(parent);

      BoundStatementBuilder boundStatementBuilder =
          updateParentsStatement
              .boundStatementBuilder()
              .setByteBuffer("state", stateBuffer)
              .setSet("new_parent", Collections.singleton(parent), ByteBuffer.class)
              .setSet("cur_parents", currentParents, ByteBuffer.class);
      ResultSet rs = session.execute(boundStatementBuilder.build());
      if (rs.wasApplied()) {
        // Success!
        break;
      } else {
        Row r = rs.one();
        assert r != null;
        currentParents = r.getSet("parents", ByteBuffer.class);
      }
    }
  }

  CompletionStage<? extends AsyncResultSet> addParentAsync(
      @NotNull ByteBuffer stateBuffer, @NotNull ByteBuffer parent) {
    // Add the parent; we do this separately from the INSERT above because multiple clients
    // may try to insert the same state at the same time (coming from different parents).
    // The concurrent inserts are idempotent (other than only one client getting final credit
    // for the state). By adding the parents afterward, via set addition, both/multiple parents
    // can be added atomically.
    BoundStatementBuilder boundStatementBuilder =
        updateParentsStatement
            .boundStatementBuilder()
            .setByteBuffer("state", stateBuffer)
            .setSet("new_parent", Collections.singleton(parent), ByteBuffer.class);
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
    CompletionStage<? extends AsyncResultSet> stage =
        session.executeAsync(
            selectStatement.boundStatementBuilder().setByteBuffer("state", state).build());
    return stage.thenApply(
        rs -> {
          Row row = rs.one();
          if (row == null) {
            return null;
          }
          return rowToPersistedBoard(row);
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
      PersistedBoard board = rowToPersistedBoard(row);
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
      boards.add(rowToPersistedBoard(row));
    }
    return boards;
  }

  @NotNull
  private PersistedBoard rowToPersistedBoard(Row row) {
    Set<ByteBuffer> persistedParents = row.getSet("parents", ByteBuffer.class);
    Set<ByteBuffer> children = row.getSet("children", ByteBuffer.class);

    assert persistedParents != null;
    assert children != null;

    byte level = row.getByte("level");
    byte bestResult = row.getByte("best_result");
    ByteBuffer state = row.getByteBuffer("state");
    assert state != null;
    return new PersistedBoard(state, persistedParents, children, level, bestResult);
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

  @SuppressWarnings("ResultOfMethodCallIgnored")
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

    client.storeBoard(b1Bits, null, null);
    client.storeBoard(b2Bits, null, b1);
    client.storeBoard(b3Bits, null, b1);
    client.storeBoard(b4Bits, null, b2);
    client.addParent(b4, b3);
    client.storeBoard(b5Bits, null, b3);
    client.storeBoard(b6Bits, null, b2);

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
