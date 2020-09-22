/*
 * Copyright (c) 2020, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core.v3;

import org.postgresql.copy.CopyOperation;
import org.postgresql.core.NativeQuery;
import org.postgresql.core.PGStream;
import org.postgresql.core.ParameterList;
import org.postgresql.core.Parser;
import org.postgresql.core.Query;
import org.postgresql.core.QueryExecutorBase;
import org.postgresql.core.ReplicationProtocol;
import org.postgresql.core.ResultCursor;
import org.postgresql.core.ResultHandler;
import org.postgresql.core.SqlCommand;
import org.postgresql.core.SqlCommandType;
import org.postgresql.jdbc.BatchResultHandler;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.logging.Logger;

public class PipelinedQuery extends QueryExecutorBase {

  private static final Logger LOGGER = Logger.getLogger(PipelinedQuery.class.getName());

  public PipelinedQuery(PGStream pgStream, String user, String database,
      int cancelSignalTimeout, Properties info) throws SQLException, IOException {
    super(pgStream, user, database, cancelSignalTimeout, info);
    readStartupMessages();
  }

  @Override
  public void execute(Query query, @Nullable ParameterList parameters, ResultHandler handler,
      int maxRows, int fetchSize, int flags) throws SQLException {

  }

  @Override
  public void execute(Query[] queries,
       @Nullable ParameterList[] parameterLists,
      BatchResultHandler handler, int maxRows, int fetchSize, int flags) throws SQLException {

  }

  @Override
  public void fetch(ResultCursor cursor, ResultHandler handler, int fetchSize) throws SQLException {

  }

  @Override
  //
  // Query parsing
  //

  public Query createSimpleQuery(String sql) throws SQLException {
    List<NativeQuery> queries = Parser.parseJdbcSql(sql,
        getStandardConformingStrings(), false, true,
        isReWriteBatchedInsertsEnabled());
    return wrap(queries);
  }

  @Override
  public Query wrap(List<NativeQuery> queries) {
    if (queries.isEmpty()) {
      // Empty query
      return emptyQuery;
    }
    if (queries.size() == 1) {
      NativeQuery firstQuery = queries.get(0);
      if (isReWriteBatchedInsertsEnabled()
          && firstQuery.getCommand().isBatchedReWriteCompatible()) {
        int valuesBraceOpenPosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceOpenPosition();
        int valuesBraceClosePosition =
            firstQuery.getCommand().getBatchRewriteValuesBraceClosePosition();
        return new BatchedQuery(firstQuery, this, valuesBraceOpenPosition,
            valuesBraceClosePosition, isColumnSanitiserDisabled());
      } else {
        return new SimpleQuery(firstQuery, this, isColumnSanitiserDisabled());
      }
    }

    // Multiple statements.
    SimpleQuery[] subqueries = new SimpleQuery[queries.size()];
    int[] offsets = new int[subqueries.length];
    int offset = 0;
    for (int i = 0; i < queries.size(); ++i) {
      NativeQuery nativeQuery = queries.get(i);
      offsets[i] = offset;
      subqueries[i] = new SimpleQuery(nativeQuery, this, isColumnSanitiserDisabled());
      offset += nativeQuery.bindPositions.length;
    }

    return new CompositeQuery(subqueries, offsets);
  }

  @Override
  public void processNotifies() throws SQLException {

  }

  @Override
  public void processNotifies(int timeoutMillis) throws SQLException {

  }

  @Override
  public ParameterList createFastpathParameters(int count) {
    return null;
  }

  @Override
  public byte @Nullable [] fastpathCall(int fnid, ParameterList params, boolean suppressBegin) throws SQLException {
    return new byte[0];
  }

  @Override
  public CopyOperation startCopy(String sql, boolean suppressBegin) throws SQLException {
    return null;
  }

  @Override
  public int getProtocolVersion() {
    return 0;
  }

  @Override
  public void setBinaryReceiveOids(Set<Integer> useBinaryForOids) {

  }

  @Override
  public void setBinarySendOids(Set<Integer> useBinaryForOids) {

  }

  @Override
  public boolean getIntegerDateTimes() {
    return false;
  }

  @Override
  public @Nullable TimeZone getTimeZone() {
    return null;
  }

  @Override
  public String getApplicationName() {
    return null;
  }

  @Override
  public ReplicationProtocol getReplicationProtocol() {
    return null;
  }

  @Override
  public boolean useBinaryForSend(int oid) {
    return false;
  }

  @Override
  public boolean useBinaryForReceive(int oid) {
    return false;
  }

  @Override
  protected void sendCloseMessage() throws IOException {
    pgStream.sendChar('X');
    pgStream.sendInteger4(4);
  }

  private final SimpleQuery beginTransactionQuery =
      new SimpleQuery(
          new NativeQuery("BEGIN", new int[0], false, SqlCommand.BLANK),
          null, false);

  private final SimpleQuery beginReadOnlyTransactionQuery =
      new SimpleQuery(
          new NativeQuery("BEGIN READ ONLY", new int[0], false, SqlCommand.BLANK),
          null, false);

  private final SimpleQuery emptyQuery =
      new SimpleQuery(
          new NativeQuery("", new int[0], false,
              SqlCommand.createStatementTypeInfo(SqlCommandType.BLANK)
          ), null, false);
}
