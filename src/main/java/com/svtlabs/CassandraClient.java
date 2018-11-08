package com.svtlabs;

import static com.svtlabs.Board.SLOTS;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class CassandraClient {
  private final PreparedStatement insertStatement;
  private final PreparedStatement selectStatement;
  private final PreparedStatement clientMetricsStatement;
  private final PreparedStatement levelMetricsStatement;
  private final PreparedStatement updateParentsStatement;

  private final CqlSession session;
  private final String clientId;

  CassandraClient(@NotNull String clientId) {
    this.clientId = clientId;

    // Connect to C*.
    session = new CqlSessionBuilder().build();
    insertStatement =
        session.prepare(
            "INSERT INTO solitaire.boards (state, children, client_id, level) VALUES (:state, :children, :client_id, :level)");
    updateParentsStatement =
        session.prepare(
            "UPDATE solitaire.boards SET parents = parents + :new_parent WHERE state = :state");
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
    session.execute(boundStatementBuilder.build());
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
    ResultSet rs =
        session.execute(
            selectStatement.boundStatementBuilder().setByteBuffer("state", state).build());
    Row row = rs.one();
    if (row == null) {
      return null;
    }
    return rowToPersistedBoard(row);
  }

  /**
   * Get all persisted boards from the db and group them by level number. NOTE: level N boards are
   * stored in index N-1.
   */
  @NotNull
  List<Collection<PersistedBoard>> getAllPersistedBoards() {
    ResultSet rs = session.execute("SELECT * FROM solitaire.boards");
    List<Collection<PersistedBoard>> boards = new ArrayList<>(SLOTS);
    for (int i = 0; i < SLOTS; i++) {
      boards.add(null);
    }

    for (Row row : rs) {
      PersistedBoard board = rowToPersistedBoard(row);
      Collection<PersistedBoard> coll = boards.get(board.getLevel() - 1);
      if (coll == null) {
        coll = new ArrayList<>();
        boards.set(board.getLevel() - 1, coll);
      }
      coll.add(board);
    }
    return boards;
  }

  @NotNull
  private PersistedBoard rowToPersistedBoard(Row row) {
    Set<ByteBuffer> persistedParents = row.getSet("parents", ByteBuffer.class);
    Set<ByteBuffer> children = row.getSet("children", ByteBuffer.class);
    byte level = row.getByte("level");
    ByteBuffer state = row.getByteBuffer("state");
    assert state != null;
    return new PersistedBoard(state, persistedParents, children, level);
  }

  void close() {
    session.close();
  }
}
