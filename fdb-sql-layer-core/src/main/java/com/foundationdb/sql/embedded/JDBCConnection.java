/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.sql.embedded;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.sql.LayerInfoInterface;
import com.foundationdb.sql.optimizer.rule.ExplainPlanContext;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.sql.server.ServerRoutineInvocation;
import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.sql.server.ServerSessionBase;
import com.foundationdb.sql.server.ServerSessionMonitor;
import com.foundationdb.sql.server.ServerTransaction;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.CallStatementNode;
import com.foundationdb.sql.parser.DDLStatementNode;
import com.foundationdb.sql.parser.DMLStatementNode;
import com.foundationdb.sql.parser.IsolationLevel;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.SQLParserException;
import com.foundationdb.sql.parser.StatementNode;

import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.error.EmbeddedResourceLeakException;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.SQLParseException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.explain.Explainable;
import com.foundationdb.server.explain.Explainer;
import com.foundationdb.server.service.monitor.MonitorStage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class JDBCConnection extends ServerSessionBase implements Connection {
    static enum CommitMode { AUTO, MANUAL, INHERITED };
    private CommitMode commitMode;
    private boolean closed;
    private JDBCWarning warnings;
    private Properties clientInfo = new Properties();
    private EmbeddedOperatorCompiler compiler;
    private List<JDBCResultSet> openResultSets = new ArrayList<>();
    private boolean setNonStandardIsolationLevel;

    private static final Logger logger = LoggerFactory.getLogger(JDBCConnection.class);
    protected static final String SERVER_TYPE = "JDBC";

    protected JDBCConnection(ServerServiceRequirements reqs, Properties info) {
        super(reqs);
        sessionMonitor = new ServerSessionMonitor(SERVER_TYPE, reqs.monitor().allocateSessionId());
        inheritFromCall();
        if ((defaultSchemaName != null) &&
            (info.getProperty("database") == null))
            info.put("database", defaultSchemaName);
        if (session == null)
            session = reqs.sessionService().createSession();
        setProperties(info);
        commitMode = (transaction != null) ? CommitMode.INHERITED : CommitMode.AUTO;
    }

    @Override
    public boolean endCall(ServerQueryContext context, 
                           ServerRoutineInvocation invocation,
                           boolean topLevel, boolean success) {
        boolean close = topLevel;
        if ((transaction != null) && (commitMode == CommitMode.MANUAL)) {
            if (success) {
                context.warnClient(new EmbeddedResourceLeakException("Connection with setAutoCommit(false) was not closed"));
                success = false; // One warning is enough.
            }
            close = true;
        }
        if (close) {
            if (!openResultSets.isEmpty()) {
                if (success) {
                    List<String> stmts = new ArrayList<>(openResultSets.size());
                    for (JDBCResultSet resultSet : openResultSets) {
                        stmts.add(resultSet.statement.sql);
                    }
                    context.warnClient(new EmbeddedResourceLeakException("ResultSet was not closed: " + stmts));
                }
                try {
                    while (!openResultSets.isEmpty()) {
                        openResultSets.get(0).close();
                    }
                }
                catch (SQLException ex) {
                    openResultSets.clear();
                }
            }
            if ((transaction != null) && (commitMode != CommitMode.INHERITED)) {
                if (success)
                    commitTransaction();
                else
                    rollbackTransaction();
            }
            deregisterSessionMonitor();
            return false;
        }
        else if (transaction != null) {
            commitMode = CommitMode.INHERITED;
            return true;
        }
        else {
            deregisterSessionMonitor(); // Just in case.
            return false;
        }
    }

    @Override
    protected void sessionChanged() {
    }

    @Override
    public void notifyClient(QueryContext.NotificationLevel level, ErrorCode errorCode, String message) {
        if (shouldNotify(level)) {
            addWarning(new JDBCWarning(level, errorCode, message));
        }
    }

    protected void addWarning(JDBCWarning warning) {
        if (warnings == null)
            warnings = warning;
        else
            warnings.setNextWarning(warning);
    }

    protected ExecutableStatement compileExecutableStatement(String sql) {
        return compileExecutableStatement(sql, false, null);
    }

    protected ExecutableStatement compileExecutableStatement(String sql,
                                                             boolean getParameterNames) {
        return compileExecutableStatement(sql, getParameterNames, null);
    }

    protected ExecutableStatement compileExecutableStatement(String sql,
                                                             ExecuteAutoGeneratedKeys autoGeneratedKeys) {
        return compileExecutableStatement(sql, false, autoGeneratedKeys);
    }

    protected ExecutableStatement compileExecutableStatement(String sql,
                                                             boolean getParameterNames,
                                                             ExecuteAutoGeneratedKeys autoGeneratedKeys) {
        logger.debug("Compile: {}", sql);
        sessionMonitor.startStatement(sql);
        EmbeddedQueryContext context = new EmbeddedQueryContext(this);
        updateAIS(context);
        boolean localTransaction = false;
        sessionMonitor.enterStage(MonitorStage.PARSE);
        try {
            StatementNode sqlStmt;
            SQLParser parser = getParser();
            try {
                sqlStmt = parser.parseStatement(sql);
            } 
            catch (SQLParserException ex) {
                throw new SQLParseException(ex);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            sessionMonitor.enterStage(MonitorStage.OPTIMIZE);
            if (transaction == null) {
                transaction = new ServerTransaction(this, true, IsolationLevel.UNSPECIFIED_ISOLATION_LEVEL, ServerTransaction.PeriodicallyCommit.OFF);
                localTransaction = true;
            }
            if ((sqlStmt instanceof DMLStatementNode) && 
                !(sqlStmt instanceof CallStatementNode))
                return compiler.compileExecutableStatement((DMLStatementNode)sqlStmt, parser.getParameterList(), getParameterNames, autoGeneratedKeys, context);
            if (autoGeneratedKeys != null)
                throw new UnsupportedOperationException();
            if (sqlStmt instanceof DDLStatementNode)
                return new ExecutableDDLStatement((DDLStatementNode)sqlStmt, sql);
            if (sqlStmt instanceof CallStatementNode)
                return ExecutableCallStatement.executableStatement((CallStatementNode)sqlStmt, parser.getParameterList(), context);
            throw new UnsupportedSQLException("Statement not recognized", sqlStmt);
        }
        finally {
            sessionMonitor.leaveStage();
            if (localTransaction)
                rollbackTransaction();
        }
    }

    public Explainer explain(String sql) {
        logger.debug("Explain: {}", sql);
        sessionMonitor.startStatement(sql);
        updateAIS(new EmbeddedQueryContext(this));
        boolean localTransaction = false;
        sessionMonitor.enterStage(MonitorStage.PARSE);
        try {
            StatementNode sqlStmt;
            SQLParser parser = getParser();
            try {
                sqlStmt = parser.parseStatement(sql);
            } 
            catch (SQLParserException ex) {
                throw new SQLParseException(ex);
            }
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            sessionMonitor.enterStage(MonitorStage.OPTIMIZE);
            if (transaction == null) {
                transaction = new ServerTransaction(this, true, IsolationLevel.UNSPECIFIED_ISOLATION_LEVEL, ServerTransaction.PeriodicallyCommit.OFF);
                localTransaction = true;
            }
            ExplainPlanContext context = new ExplainPlanContext(compiler, new EmbeddedQueryContext(this));
            Explainable explainable;
            if ((sqlStmt instanceof DMLStatementNode) && 
                !(sqlStmt instanceof CallStatementNode))
                explainable = compiler.compile((DMLStatementNode)sqlStmt, parser.getParameterList(), context).getPlannable();
            else
                throw new UnsupportedSQLException("Statement not supported for EXPLAIN", sqlStmt);
            return explainable.getExplainer(context.getExplainContext());
        }
        finally {
            sessionMonitor.leaveStage();
            if (localTransaction)
                rollbackTransaction();
        }
    }

    protected void updateAIS(EmbeddedQueryContext context) {
        DDLFunctions ddl = reqs.dxl().ddlFunctions();
        AkibanInformationSchema newAIS = ddl.getAIS(session);
        if ((ais != null) && (ais.getGeneration() == newAIS.getGeneration()))
            return;             // Unchanged.
        ais = newAIS;
        rebuildCompiler();
    }

    protected void rebuildCompiler() {
        initParser();
        compiler = EmbeddedOperatorCompiler.create(this, reqs.store());
        initAdapters(compiler);
    }

    // Slightly different contract than ServerSessionBase, since a transaction
    // remains open when a until its read result set is closed.
    protected void beforeExecuteStatement(ExecutableStatement stmt) throws SQLException {
        sessionMonitor.enterStage(MonitorStage.EXECUTE);
        boolean localTransaction;
        try {
            localTransaction = super.beforeExecute(stmt);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
        if (localTransaction) {
            logger.debug("Auto BEGIN TRANSACTION");
            registerSessionMonitor();
        }
    }

    protected void afterExecuteStatement(ExecutableStatement stmt, boolean success) throws SQLException {
        sessionMonitor.leaveStage();
        boolean localTransaction = false;
        if (checkAutoCommit()) {
            // An update statement without any open cursors, or a
            // query statement that fails before the cursor gets fully
            // set up. Treat as local transaction and commit / abort
            // now.
            localTransaction = true;
            deregisterSessionMonitor();
            logger.debug(success ? "Auto COMMIT TRANSACTION" : "Auto ROLLBACK TRANSACTION");
        }
        try {
            super.afterExecute(stmt, localTransaction, success, true);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    protected void openingResultSet(JDBCResultSet resultSet) {
        assert isTransactionActive();
        openResultSets.add(resultSet);
    }

    protected void closingResultSet(JDBCResultSet resultSet) {
        openResultSets.remove(resultSet);
        if (checkAutoCommit()) {
            commitTransaction();
            deregisterSessionMonitor();
            logger.debug("Auto COMMIT TRANSACTION");
        }
    }

    protected boolean checkAutoCommit() {
        return ((commitMode == CommitMode.AUTO) && 
                (transaction != null) &&
                openResultSets.isEmpty());
    }

    // Register as a result of beginning a transaction (which is implicit).
    protected void registerSessionMonitor() {
        reqs.monitor().registerSessionMonitor(sessionMonitor, session);
    }

    // Deregister when transaction is committed, rolled back, or connection closed.
    protected void deregisterSessionMonitor() {
        reqs.monitor().deregisterSessionMonitor(sessionMonitor, session);
    }

    protected LayerInfoInterface getLayerInfo() {
        return reqs.layerInfo();
    }

    public JDBCCallableStatement prepareCall(TableName routineName) throws SQLException {
        EmbeddedQueryContext context = new EmbeddedQueryContext(this);
        updateAIS(context);
        return new JDBCCallableStatement(this, routineName.toString(), ExecutableCallStatement.executableStatement(routineName, context));
    }

    /* Wrapper */

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    /* Connection */

    @Override
    public Statement createStatement() throws SQLException {
        return new JDBCStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new JDBCPreparedStatement(this, sql, compileExecutableStatement(sql));
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return new JDBCCallableStatement(this, sql, compileExecutableStatement(sql, true));
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        switch (commitMode) {
        case INHERITED:
            throw new JDBCException("Cannot set auto commit with outer transaction", ErrorCode.AUTO_COMMIT_USAGE);
        case AUTO:
            if (!autoCommit)
                commitMode = CommitMode.MANUAL;
            break;
        case MANUAL:
            if (autoCommit)
                commitMode = CommitMode.AUTO;
            checkAutoCommit();  // Commit now if no open cursors.
            break;
        }
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return (commitMode == CommitMode.AUTO);
    }

    @Override
    public void commit() throws SQLException {
        switch (commitMode) {
        case AUTO:
            throw new JDBCException("Commit not allowed in auto-commit mode", ErrorCode.AUTO_COMMIT_USAGE);
        case INHERITED:
            throw new JDBCException("Commit not allowed with outer transaction", ErrorCode.AUTO_COMMIT_USAGE);
        }
        try {
            commitTransaction();
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
        if (openResultSets.isEmpty())
            deregisterSessionMonitor();
    }

    @Override
    public void rollback() throws SQLException {
        switch (commitMode) {
        case AUTO:
            throw new JDBCException("Rollback not allowed in auto-commit mode", ErrorCode.AUTO_COMMIT_USAGE);
        case INHERITED:
            throw new JDBCException("Rollback not allowed with outer transaction", ErrorCode.AUTO_COMMIT_USAGE);
        }
        try {
            rollbackTransaction();
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
        if (openResultSets.isEmpty())
            deregisterSessionMonitor();
    }

    @Override
    public void close() throws SQLException {
        if (isTransactionActive() && (commitMode != CommitMode.INHERITED))
            rollbackTransaction();
        while (!openResultSets.isEmpty()) {
            openResultSets.get(0).close();
        }
        deregisterSessionMonitor();
        this.closed = true;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new JDBCDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        this.transactionDefaultReadOnly = readOnly;
        if (transaction != null)
            transaction.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return transactionDefaultReadOnly;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    /** Non-standard values using the same API.
     * TODO: Would a whole new method be better?
     */
    public static final int TRANSACTION_READ_COMMITTED_NO_SNAPSHOT = -2;
    public static final int TRANSACTION_SERIALIZABLE_SNAPSHOT = -8;

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        // Remember so get returns this, too.
        if (level < 0)
            setNonStandardIsolationLevel = true;
        IsolationLevel ilevel;
        switch (level) {
        case TRANSACTION_READ_COMMITTED:
            ilevel = IsolationLevel.READ_COMMITTED_ISOLATION_LEVEL;
            break;
        case TRANSACTION_READ_UNCOMMITTED:
            ilevel = IsolationLevel.READ_UNCOMMITTED_ISOLATION_LEVEL;
            break;
        case TRANSACTION_REPEATABLE_READ:
            ilevel = IsolationLevel.REPEATABLE_READ_ISOLATION_LEVEL;
            break;
        case TRANSACTION_SERIALIZABLE:
            ilevel = IsolationLevel.SERIALIZABLE_ISOLATION_LEVEL;
            break;
        case TRANSACTION_READ_COMMITTED_NO_SNAPSHOT:
            ilevel = IsolationLevel.READ_COMMITTED_NO_SNAPSHOT_ISOLATION_LEVEL;
            break;
        case TRANSACTION_SERIALIZABLE_SNAPSHOT:
            ilevel = IsolationLevel.SNAPSHOT_ISOLATION_LEVEL;
            break;
        default:
            throw new SQLException("Unknown isolation level " + level);
        }
        if (isTransactionActive())
            ilevel = setTransactionIsolationLevel(ilevel);
        setTransactionDefaultIsolationLevel(ilevel);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        IsolationLevel level = transactionDefaultIsolationLevel;
        if (level == IsolationLevel.UNSPECIFIED_ISOLATION_LEVEL)
            level = getTransactionService().actualIsolationLevel(level);
        switch (level) {
        case READ_COMMITTED_NO_SNAPSHOT_ISOLATION_LEVEL:
            if (setNonStandardIsolationLevel)
                return TRANSACTION_READ_COMMITTED_NO_SNAPSHOT;
            /* else falls through */
        case READ_UNCOMMITTED_ISOLATION_LEVEL:
            return TRANSACTION_READ_UNCOMMITTED;
        case READ_COMMITTED_ISOLATION_LEVEL:
            return TRANSACTION_READ_COMMITTED;
        case SNAPSHOT_ISOLATION_LEVEL:
            if (setNonStandardIsolationLevel)
                return TRANSACTION_SERIALIZABLE_SNAPSHOT;
            /* else falls through */
        case REPEATABLE_READ_ISOLATION_LEVEL:
            return TRANSACTION_REPEATABLE_READ;
        case SERIALIZABLE_ISOLATION_LEVEL:
            return TRANSACTION_SERIALIZABLE;
        default:
            return TRANSACTION_NONE;
        }
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    @Override
    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return prepareCall(sql);
    }

    @Override
    public Map<String,Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setTypeMap(java.util.Map<String,Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        return new JDBCPreparedStatement(this, sql,
                                         compileExecutableStatement(sql, 
                                                                    ExecuteAutoGeneratedKeys.of(autoGeneratedKeys)));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
            throws SQLException {
        return new JDBCPreparedStatement(this, sql,
                                         compileExecutableStatement(sql, 
                                                                    ExecuteAutoGeneratedKeys.of(columnIndexes)));
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames)
            throws SQLException {
        return new JDBCPreparedStatement(this, sql,
                                         compileExecutableStatement(sql, 
                                                                    ExecuteAutoGeneratedKeys.of(columnNames)));
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.put(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        setDefaultSchemaName(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getDefaultSchemaName();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
