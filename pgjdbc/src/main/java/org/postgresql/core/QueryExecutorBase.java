/*
 * Copyright (c) 2003, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.core;

import org.postgresql.PGNotification;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.AutoSave;
import org.postgresql.jdbc.EscapeSyntaxCallMode;
import org.postgresql.jdbc.PreferQueryMode;
import org.postgresql.jdbc.TimestampUtils;
import org.postgresql.util.GT;
import org.postgresql.util.HostSpec;
import org.postgresql.util.LruCache;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;
import org.postgresql.util.PSQLWarning;
import org.postgresql.util.ServerErrorMessage;

import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class QueryExecutorBase implements QueryExecutor {

  private static final Logger LOGGER = Logger.getLogger(QueryExecutorBase.class.getName());
  protected final PGStream pgStream;
  private final String user;
  private final String database;
  private final int cancelSignalTimeout;

  private int cancelPid;
  private int cancelKey;
  private boolean closed = false;
  private @MonotonicNonNull String serverVersion;
  private int serverVersionNum = 0;
  private TransactionState transactionState = TransactionState.IDLE;
  private final boolean reWriteBatchedInserts;
  private final boolean columnSanitiserDisabled;
  private final EscapeSyntaxCallMode escapeSyntaxCallMode;
  private final PreferQueryMode preferQueryMode;
  private AutoSave autoSave;
  private boolean flushCacheOnDeallocate = true;
  protected final boolean logServerErrorDetail;
  protected final boolean allowEncodingChanges;

  // default value for server versions that don't report standard_conforming_strings
  private boolean standardConformingStrings = false;

  private @Nullable SQLWarning warnings;
  private final ArrayList<PGNotification> notifications = new ArrayList<PGNotification>();

  private final LruCache<Object, CachedQuery> statementCache;
  private final CachedQueryCreateAction cachedQueryCreateAction;

  // For getParameterStatuses(), GUC_REPORT tracking
  private final TreeMap<String,String> parameterStatuses
      = new TreeMap<String,String>(String.CASE_INSENSITIVE_ORDER);

  /**
   * The exception that caused the last transaction to fail.
   */
  protected  @Nullable SQLException transactionFailCause;

  /**
   * TimeZone of the current connection (TimeZone backend parameter).
   */
  protected  @Nullable TimeZone timeZone;

  /**
   * application_name connection property.
   */
  protected  @Nullable String applicationName;

  /**
   * True if server uses integers for date and time fields. False if server uses double.
   */
  private boolean integerDateTimes;

  @SuppressWarnings({"assignment.type.incompatible", "argument.type.incompatible"})
  protected QueryExecutorBase(PGStream pgStream, String user,
      String database, int cancelSignalTimeout, Properties info) throws SQLException {
    this.pgStream = pgStream;
    this.user = user;
    this.database = database;
    this.cancelSignalTimeout = cancelSignalTimeout;
    this.allowEncodingChanges = PGProperty.ALLOW_ENCODING_CHANGES.getBoolean(info);
    this.reWriteBatchedInserts = PGProperty.REWRITE_BATCHED_INSERTS.getBoolean(info);
    this.columnSanitiserDisabled = PGProperty.DISABLE_COLUMN_SANITISER.getBoolean(info);
    String callMode = PGProperty.ESCAPE_SYNTAX_CALL_MODE.get(info);
    this.escapeSyntaxCallMode = EscapeSyntaxCallMode.of(callMode);
    String preferMode = PGProperty.PREFER_QUERY_MODE.get(info);
    this.preferQueryMode = PreferQueryMode.of(preferMode);
    this.autoSave = AutoSave.of(PGProperty.AUTOSAVE.get(info));
    this.logServerErrorDetail = PGProperty.LOG_SERVER_ERROR_DETAIL.getBoolean(info);
    // assignment.type.incompatible, argument.type.incompatible
    this.cachedQueryCreateAction = new CachedQueryCreateAction(this);
    statementCache = new LruCache<Object, CachedQuery>(
        Math.max(0, PGProperty.PREPARED_STATEMENT_CACHE_QUERIES.getInt(info)),
        Math.max(0, PGProperty.PREPARED_STATEMENT_CACHE_SIZE_MIB.getInt(info) * 1024 * 1024),
        false,
        cachedQueryCreateAction,
        new LruCache.EvictAction<CachedQuery>() {
          @Override
          public void evict(CachedQuery cachedQuery) throws SQLException {
            cachedQuery.query.close();
          }
        });
  }

  protected abstract void sendCloseMessage() throws IOException;

  @Override
  public void setNetworkTimeout(int milliseconds) throws IOException {
    pgStream.setNetworkTimeout(milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws IOException {
    return pgStream.getNetworkTimeout();
  }

  @Override
  public HostSpec getHostSpec() {
    return pgStream.getHostSpec();
  }

  @Override
  public String getUser() {
    return user;
  }

  @Override
  public String getDatabase() {
    return database;
  }

  public void setBackendKeyData(int cancelPid, int cancelKey) {
    this.cancelPid = cancelPid;
    this.cancelKey = cancelKey;
  }

  @Override
  public int getBackendPID() {
    return cancelPid;
  }

  @Override
  public void abort() {
    try {
      pgStream.getSocket().close();
    } catch (IOException e) {
      // ignore
    }
    closed = true;
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    try {
      LOGGER.log(Level.FINEST, " FE=> Terminate");
      sendCloseMessage();
      pgStream.flush();
      pgStream.close();
    } catch (IOException ioe) {
      LOGGER.log(Level.FINEST, "Discarding IOException on close:", ioe);
    }

    closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public void sendQueryCancel() throws SQLException {
    if (cancelPid <= 0) {
      return;
    }

    PGStream cancelStream = null;

    // Now we need to construct and send a cancel packet
    try {
      if (LOGGER.isLoggable(Level.FINEST)) {
        LOGGER.log(Level.FINEST, " FE=> CancelRequest(pid={0},ckey={1})", new Object[]{cancelPid, cancelKey});
      }

      cancelStream =
          new PGStream(pgStream.getSocketFactory(), pgStream.getHostSpec(), cancelSignalTimeout);
      if (cancelSignalTimeout > 0) {
        cancelStream.getSocket().setSoTimeout(cancelSignalTimeout);
      }
      cancelStream.sendInteger4(16);
      cancelStream.sendInteger2(1234);
      cancelStream.sendInteger2(5678);
      cancelStream.sendInteger4(cancelPid);
      cancelStream.sendInteger4(cancelKey);
      cancelStream.flush();
      cancelStream.receiveEOF();
    } catch (IOException e) {
      // Safe to ignore.
      LOGGER.log(Level.FINEST, "Ignoring exception on cancel request:", e);
    } finally {
      if (cancelStream != null) {
        try {
          cancelStream.close();
        } catch (IOException e) {
          // Ignored.
        }
      }
    }
  }

  public synchronized void addWarning(SQLWarning newWarning) {
    if (warnings == null) {
      warnings = newWarning;
    } else {
      warnings.setNextWarning(newWarning);
    }
  }

  public synchronized void addNotification(PGNotification notification) {
    notifications.add(notification);
  }

  @Override
  public synchronized PGNotification[] getNotifications() throws SQLException {
    PGNotification[] array = notifications.toArray(new PGNotification[0]);
    notifications.clear();
    return array;
  }

  @Override
  public synchronized @Nullable SQLWarning getWarnings() {
    SQLWarning chain = warnings;
    warnings = null;
    return chain;
  }

  @Override
  public String getServerVersion() {
    String serverVersion = this.serverVersion;
    if (serverVersion == null) {
      throw new IllegalStateException("serverVersion must not be null");
    }
    return serverVersion;
  }

  @Override
  public int getServerVersionNum() {
    if (serverVersionNum != 0) {
      return serverVersionNum;
    }
    return serverVersionNum = Utils.parseServerVersionStr(getServerVersion());
  }

  public void setServerVersion(String serverVersion) {
    this.serverVersion = serverVersion;
  }

  public void setServerVersionNum(int serverVersionNum) {
    this.serverVersionNum = serverVersionNum;
  }

  public synchronized void setTransactionState(TransactionState state) {
    transactionState = state;
  }

  public synchronized void setStandardConformingStrings(boolean value) {
    standardConformingStrings = value;
  }

  @Override
  public synchronized boolean getStandardConformingStrings() {
    return standardConformingStrings;
  }

  @Override
  public synchronized TransactionState getTransactionState() {
    return transactionState;
  }

  public void setEncoding(Encoding encoding) throws IOException {
    pgStream.setEncoding(encoding);
  }

  @Override
  public Encoding getEncoding() {
    return pgStream.getEncoding();
  }

  @Override
  public boolean isReWriteBatchedInsertsEnabled() {
    return this.reWriteBatchedInserts;
  }

  @Override
  public final CachedQuery borrowQuery(String sql) throws SQLException {
    return statementCache.borrow(sql);
  }

  @Override
  public final CachedQuery borrowCallableQuery(String sql) throws SQLException {
    return statementCache.borrow(new CallableQueryKey(sql));
  }

  @Override
  public final CachedQuery borrowReturningQuery(String sql, String @Nullable [] columnNames)
      throws SQLException {
    return statementCache.borrow(new QueryWithReturningColumnsKey(sql, true, true,
        columnNames
    ));
  }

  @Override
  public CachedQuery borrowQueryByKey(Object key) throws SQLException {
    return statementCache.borrow(key);
  }

  @Override
  public void releaseQuery(CachedQuery cachedQuery) {
    statementCache.put(cachedQuery.key, cachedQuery);
  }

  @Override
  public final Object createQueryKey(String sql, boolean escapeProcessing,
      boolean isParameterized, String @Nullable ... columnNames) {
    Object key;
    if (columnNames == null || columnNames.length != 0) {
      // Null means "return whatever sensible columns are" (e.g. primary key, or serial, or something like that)
      key = new QueryWithReturningColumnsKey(sql, isParameterized, escapeProcessing, columnNames);
    } else if (isParameterized) {
      // If no generated columns requested, just use the SQL as a cache key
      key = sql;
    } else {
      key = new BaseQueryKey(sql, false, escapeProcessing);
    }
    return key;
  }

  @Override
  public CachedQuery createQueryByKey(Object key) throws SQLException {
    return cachedQueryCreateAction.create(key);
  }

  @Override
  public final CachedQuery createQuery(String sql, boolean escapeProcessing,
      boolean isParameterized, String @Nullable ... columnNames)
      throws SQLException {
    Object key = createQueryKey(sql, escapeProcessing, isParameterized, columnNames);
    // Note: cache is not reused here for two reasons:
    //   1) Simplify initial implementation for simple statements
    //   2) Non-prepared statements are likely to have literals, thus query reuse would not be often
    return createQueryByKey(key);
  }

  @Override
  public boolean isColumnSanitiserDisabled() {
    return columnSanitiserDisabled;
  }

  @Override
  public EscapeSyntaxCallMode getEscapeSyntaxCallMode() {
    return escapeSyntaxCallMode;
  }

  @Override
  public PreferQueryMode getPreferQueryMode() {
    return preferQueryMode;
  }

  public AutoSave getAutoSave() {
    return autoSave;
  }

  public void setAutoSave(AutoSave autoSave) {
    this.autoSave = autoSave;
  }

  protected boolean willHealViaReparse(SQLException e) {
    if (e == null || e.getSQLState() == null) {
      return false;
    }

    // "prepared statement \"S_2\" does not exist"
    if (PSQLState.INVALID_SQL_STATEMENT_NAME.getState().equals(e.getSQLState())) {
      return true;
    }
    if (!PSQLState.NOT_IMPLEMENTED.getState().equals(e.getSQLState())) {
      return false;
    }

    if (!(e instanceof PSQLException)) {
      return false;
    }

    PSQLException pe = (PSQLException) e;

    ServerErrorMessage serverErrorMessage = pe.getServerErrorMessage();
    if (serverErrorMessage == null) {
      return false;
    }
    // "cached plan must not change result type"
    String routine = serverErrorMessage.getRoutine();
    return "RevalidateCachedQuery".equals(routine) // 9.2+
        || "RevalidateCachedPlan".equals(routine); // <= 9.1
  }

  @Override
  public boolean willHealOnRetry(SQLException e) {
    if (autoSave == AutoSave.NEVER && getTransactionState() == TransactionState.FAILED) {
      // If autorollback is not activated, then every statement will fail with
      // 'transaction is aborted', etc, etc
      return false;
    }
    return willHealViaReparse(e);
  }

  public boolean isFlushCacheOnDeallocate() {
    return flushCacheOnDeallocate;
  }

  public void setFlushCacheOnDeallocate(boolean flushCacheOnDeallocate) {
    this.flushCacheOnDeallocate = flushCacheOnDeallocate;
  }

  protected boolean hasNotifications() {
    return notifications.size() > 0;
  }

  @Override
  public final Map<String,String> getParameterStatuses() {
    return Collections.unmodifiableMap(parameterStatuses);
  }

  @Override
  public final @Nullable String getParameterStatus(String parameterName) {
    return parameterStatuses.get(parameterName);
  }

  /**
   * Update the parameter status map in response to a new ParameterStatus
   * wire protocol message.
   *
   * <p>The server sends ParameterStatus messages when GUC_REPORT settings are
   * initially assigned and whenever they change.</p>
   *
   * <p>A future version may invoke a client-defined listener class at this point,
   * so this should be the only access path.</p>
   *
   * <p>Keys are case-insensitive and case-preserving.</p>
   *
   * <p>The server doesn't provide a way to report deletion of a reportable
   * parameter so we don't expose one here.</p>
   *
   * @param parameterName case-insensitive case-preserving name of parameter to create or update
   * @param parameterStatus new value of parameter
   * @see org.postgresql.PGConnection#getParameterStatuses
   * @see org.postgresql.PGConnection#getParameterStatus
   */
  protected void onParameterStatus(String parameterName, String parameterStatus) {
    if (parameterName == null || parameterName.equals("")) {
      throw new IllegalStateException("attempt to set GUC_REPORT parameter with null or empty-string name");
    }

    parameterStatuses.put(parameterName, parameterStatus);
  }

  /**
   * We really only have one version now
   * @return
   */
  public int getProtocolVersion() {
    return 3;
  }

  protected void receiveRFQ() throws IOException {
    if (pgStream.receiveInteger4() != 5) {
      throw new IOException("unexpected length of ReadyForQuery message");
    }

    char tStatus = (char) pgStream.receiveChar();
    if (LOGGER.isLoggable(Level.FINEST)) {
      LOGGER.log(Level.FINEST, " <=BE ReadyForQuery({0})", tStatus);
    }

    // Update connection state.
    switch (tStatus) {
      case 'I':
        transactionFailCause = null;
        setTransactionState(TransactionState.IDLE);
        break;
      case 'T':
        transactionFailCause = null;
        setTransactionState(TransactionState.OPEN);
        break;
      case 'E':
        setTransactionState(TransactionState.FAILED);
        break;
      default:
        throw new IOException(
            "unexpected transaction state in ReadyForQuery message: " + (int) tStatus);
    }
  }

  protected SQLException receiveErrorResponse() throws IOException {
    // it's possible to get more than one error message for a query
    // see libpq comments wrt backend closing a connection
    // so, append messages to a string buffer and keep processing
    // check at the bottom to see if we need to throw an exception

    int elen = pgStream.receiveInteger4();
    assert elen > 4 : "Error response length must be greater than 4";

    EncodingPredictor.DecodeResult totalMessage = pgStream.receiveErrorString(elen - 4);
    ServerErrorMessage errorMsg = new ServerErrorMessage(totalMessage);

    if (LOGGER.isLoggable(Level.FINEST)) {
      LOGGER.log(Level.FINEST, " <=BE ErrorMessage({0})", errorMsg.toString());
    }

    PSQLException error = new PSQLException(errorMsg, this.logServerErrorDetail);
    if (transactionFailCause == null) {
      transactionFailCause = error;
    } else {
      error.initCause(transactionFailCause);
    }
    return error;
  }

  protected SQLWarning receiveNoticeResponse() throws IOException {
    int nlen = pgStream.receiveInteger4();
    assert nlen > 4 : "Notice Response length must be greater than 4";

    ServerErrorMessage warnMsg = new ServerErrorMessage(pgStream.receiveString(nlen - 4));

    if (LOGGER.isLoggable(Level.FINEST)) {
      LOGGER.log(Level.FINEST, " <=BE NoticeResponse({0})", warnMsg.toString());
    }

    return new PSQLWarning(warnMsg);
  }

  protected String receiveCommandStatus() throws IOException {
    // TODO: better handle the msg len
    int len = pgStream.receiveInteger4();
    // read len -5 bytes (-4 for len and -1 for trailing \0)
    String status = pgStream.receiveString(len - 5);
    // now read and discard the trailing \0
    pgStream.receiveChar(); // Receive(1) would allocate new byte[1], so avoid it

    LOGGER.log(Level.FINEST, " <=BE CommandStatus({0})", status);

    return status;
  }

  protected void receiveParameterStatus() throws IOException, SQLException {
    // ParameterStatus
    pgStream.receiveInteger4(); // MESSAGE SIZE
    String name = pgStream.receiveString();
    String value = pgStream.receiveString();

    if (LOGGER.isLoggable(Level.FINEST)) {
      LOGGER.log(Level.FINEST, " <=BE ParameterStatus({0} = {1})", new Object[]{name, value});
    }

    /* Update client-visible parameter status map for getParameterStatuses() */
    if (name != null && !name.equals("")) {
      onParameterStatus(name, value);
    }

    if (name.equals("client_encoding")) {
      if (allowEncodingChanges) {
        if (!value.equalsIgnoreCase("UTF8") && !value.equalsIgnoreCase("UTF-8")) {
          LOGGER.log(Level.FINE,
              "pgjdbc expects client_encoding to be UTF8 for proper operation. Actual encoding is {0}",
              value);
        }
        pgStream.setEncoding(Encoding.getDatabaseEncoding(value));
      } else if (!value.equalsIgnoreCase("UTF8") && !value.equalsIgnoreCase("UTF-8")) {
        close(); // we're screwed now; we can't trust any subsequent string.
        throw new PSQLException(GT.tr(
            "The server''s client_encoding parameter was changed to {0}. The JDBC driver requires client_encoding to be UTF8 for correct operation.",
            value), PSQLState.CONNECTION_FAILURE);

      }
    }

    if (name.equals("DateStyle") && !value.startsWith("ISO")
        && !value.toUpperCase().startsWith("ISO")) {
      close(); // we're screwed now; we can't trust any subsequent date.
      throw new PSQLException(GT.tr(
          "The server''s DateStyle parameter was changed to {0}. The JDBC driver requires DateStyle to begin with ISO for correct operation.",
          value), PSQLState.CONNECTION_FAILURE);
    }

    if (name.equals("standard_conforming_strings")) {
      if (value.equals("on")) {
        setStandardConformingStrings(true);
      } else if (value.equals("off")) {
        setStandardConformingStrings(false);
      } else {
        close();
        // we're screwed now; we don't know how to escape string literals
        throw new PSQLException(GT.tr(
            "The server''s standard_conforming_strings parameter was reported as {0}. The JDBC driver expected on or off.",
            value), PSQLState.CONNECTION_FAILURE);
      }
      return;
    }

    if ("TimeZone".equals(name)) {
      setTimeZone(TimestampUtils.parseBackendTimeZone(value));
    } else if ("application_name".equals(name)) {
      setApplicationName(value);
    } else if ("server_version_num".equals(name)) {
      setServerVersionNum(Integer.parseInt(value));
    } else if ("server_version".equals(name)) {
      setServerVersion(value);
    }  else if ("integer_datetimes".equals(name)) {
      if ("on".equals(value)) {
        setIntegerDateTimes(true);
      } else if ("off".equals(value)) {
        setIntegerDateTimes(false);
      } else {
        throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
            PSQLState.PROTOCOL_VIOLATION);
      }
    }
  }

  public void setTimeZone(TimeZone timeZone) {
    this.timeZone = timeZone;
  }

  public @Nullable TimeZone getTimeZone() {
    return timeZone;
  }

  public void setApplicationName(String applicationName) {
    this.applicationName = applicationName;
  }

  public String getApplicationName() {
    if (applicationName == null) {
      return "";
    }
    return applicationName;
  }

  public void readStartupMessages() throws IOException, SQLException {
    for (int i = 0; i < 1000; i++) {
      int beresp = pgStream.receiveChar();
      switch (beresp) {
        case 'Z':
          receiveRFQ();
          // Ready For Query; we're done.
          return;

        case 'K':
          // BackendKeyData
          int msgLen = pgStream.receiveInteger4();
          if (msgLen != 12) {
            throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
                PSQLState.PROTOCOL_VIOLATION);
          }

          int pid = pgStream.receiveInteger4();
          int ckey = pgStream.receiveInteger4();

          if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, " <=BE BackendKeyData(pid={0},ckey={1})", new Object[]{pid, ckey});
          }

          setBackendKeyData(pid, ckey);
          break;

        case 'E':
          // Error
          throw receiveErrorResponse();

        case 'N':
          // Warning
          addWarning(receiveNoticeResponse());
          break;

        case 'S':
          // ParameterStatus
          receiveParameterStatus();

          break;

        default:
          if (LOGGER.isLoggable(Level.FINEST)) {
            LOGGER.log(Level.FINEST, "  invalid message type={0}", (char) beresp);
          }
          throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
              PSQLState.PROTOCOL_VIOLATION);
      }
    }
    throw new PSQLException(GT.tr("Protocol error.  Session setup failed."),
        PSQLState.PROTOCOL_VIOLATION);
  }

  protected void setIntegerDateTimes(boolean state) {
    integerDateTimes = state;
  }

  public boolean getIntegerDateTimes() {
    return integerDateTimes;
  }

}
