package com.svtlabs;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
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
import java.util.concurrent.Semaphore;
import org.jetbrains.annotations.NotNull;

class CassandraClient {
  private final PreparedStatement insertWinningBoardStatement;
  private final PreparedStatement insertRelationStatement;
  private final PreparedStatement selectParentsStatement;

  private final CqlSession session;
  private final Semaphore asyncRequestSemaphore;
  private final String clientId;

  CassandraClient(@NotNull String clientId, @NotNull String seed) {
    this.clientId = clientId;

    // Connect to C*.

    session = new CqlSessionBuilder().addContactPoint(new InetSocketAddress(seed, 9042)).build();
    asyncRequestSemaphore =
        new Semaphore(
            session
                .getContext()
                .getConfig()
                .getDefaultProfile()
                .getInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS));
    insertWinningBoardStatement =
        session.prepare("INSERT INTO solitaire.winning_boards (state) VALUES (:state)");
    insertRelationStatement =
        session.prepare("INSERT INTO solitaire.board_rel (child, parent) VALUES (:child, :parent)");
    selectParentsStatement =
        session.prepare("SELECT parent FROM solitaire.board_rel WHERE child = :child");
  }

  CompletionStage<? extends AsyncResultSet> addBoardRelation(ByteBuffer child, ByteBuffer parent) {
    BoundStatementBuilder boundStatementBuilder =
        insertRelationStatement
            .boundStatementBuilder()
            .setByteBuffer("child", child)
            .setByteBuffer("parent", parent);

    asyncRequestSemaphore.acquireUninterruptibly();
    return session
        .executeAsync(boundStatementBuilder.build())
        .whenComplete((result, excp) -> asyncRequestSemaphore.release());
  }

  CompletionStage<? extends AsyncResultSet> storeWinningBoard(@NotNull BitSet state) {
    ByteBuffer stateBuffer = ByteBuffer.wrap(state.toByteArray());
    BoundStatementBuilder boundStatementBuilder =
        insertWinningBoardStatement.boundStatementBuilder().setByteBuffer("state", stateBuffer);
    asyncRequestSemaphore.acquireUninterruptibly();
    return session
        .executeAsync(boundStatementBuilder.build())
        .whenComplete((result, excp) -> asyncRequestSemaphore.release());
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

  @NotNull
  private Board rowToBoard(Row row, String fieldName) {
    ByteBuffer state = row.getByteBuffer(fieldName);
    assert state != null;
    return new Board(state);
  }

  void close() {
    session.close();
  }
}
