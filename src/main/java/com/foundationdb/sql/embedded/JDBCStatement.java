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

import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.sql.embedded.JDBCException.Wrapper;

import java.sql.*;
import java.util.ArrayList;
import java.util.Queue;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JDBCStatement implements Statement
{

    private static final Logger LOG = LoggerFactory.getLogger(JDBCStatement.class.getName());

    protected final JDBCConnection connection;
    protected String sql;
    private boolean closed;
    private JDBCWarning warnings;
    private int currentUpdateCount;
    // Note that result sets need not be for this connection. For
    // example, if a stored procedure with dynamic result sets called
    // is, we don't know where its results came
    // from. secondaryResultSets are always from the same connection;
    // but this is only how they are set up.
    private ResultSet currentResultSet, generatedKeys;
    private List<ResultSet> secondaryResultSets; // For instance, nested.
    private Queue<ResultSet> pendingResultSets; // For instance, from stored procedure.

    protected JDBCStatement(JDBCConnection connection) {
        this.connection = connection;
    }

    public boolean executeInternal(ExecutableStatement stmt, EmbeddedQueryContext context, QueryBindings bindings) 
            throws SQLException {
        if (context == null) {
            if (stmt.getParameterMetaData() != null)
                throw new JDBCException("Statement requires parameters; must prepare");
            context = new EmbeddedQueryContext(this);
        }
        if (bindings == null) {
            bindings = context.createBindings();
        }
        boolean hasResultSet = false;
        connection.beforeExecuteStatement(stmt);
        boolean success = false;
        try {
            ExecuteResults results = stmt.execute(context, bindings);
            currentUpdateCount = results.getUpdateCount();
            if (results.getCursor() != null) {
                JDBCResultSet resultSet = new JDBCResultSet(this, stmt.getResultSetMetaData(), results.getCursor());
                if (results.hasResultSet()) {
                    // Cursor is ordinary result. This will keep an
                    // auto-commit transaction open until it is
                    // closed.
                    connection.openingResultSet(resultSet);
                    currentResultSet = resultSet;
                    hasResultSet = true;
                }
                else {
                    // These are copied (to get update count) and do
                    // not need a transaction. Note that behavior of
                    // generated keys is explicitly ill-defined by the
                    // JDBC spec in auto-commit mode.
                    generatedKeys = resultSet;
                }
            }
            else if (results.getAdditionalResultSets() != null) {
                pendingResultSets = results.getAdditionalResultSets();
                hasResultSet = getMoreResults();
            }
            success = true;
        }
        catch (RuntimeException ex) {
            Throwable throwable = ex;
            if (throwable instanceof Wrapper) {
                throwable = (SQLException)ex.getCause();
            }

            final ErrorCode code = ErrorCode.getCodeForRESTException(throwable);
            code.logAtImportance(
                    LOG, "Statement execution for query {} failed with exception {}", sql, throwable
            );

            throw JDBCException.throwUnwrapped(ex);
        }
        finally {
            connection.afterExecuteStatement(stmt, success);
        }
        return hasResultSet;
    }

    public ResultSet executeQueryInternal(ExecutableStatement stmt, 
                                          EmbeddedQueryContext context,
                                          QueryBindings bindings)
            throws SQLException {
        boolean hasResultSet = executeInternal(stmt, context, bindings);
        if (!hasResultSet) throw new JDBCException("Statement is not SELECT");
        return getResultSet();
    }

    public int executeUpdateInternal(ExecutableStatement stmt, 
                                     EmbeddedQueryContext context,
                                     QueryBindings bindings)
            throws SQLException {
        boolean hasResultSet = executeInternal(stmt, context, bindings);
        if (hasResultSet) throw new JDBCException("Statement is SELECT");
        return getUpdateCount();
    }
    
    protected void addWarning(JDBCWarning warning) {
        if (warnings == null)
            warnings = warning;
        else
            warnings.setNextWarning(warning);
    }

    protected void secondaryResultSet(JDBCResultSet resultSet) {
        if (secondaryResultSets == null)
            secondaryResultSets = new ArrayList<>();
        secondaryResultSets.add(resultSet);
        connection.openingResultSet(resultSet);
    }

    protected void closingResultSet(JDBCResultSet resultSet) {
        if (currentResultSet == resultSet)
            currentResultSet = null;
        if (secondaryResultSets != null)
            secondaryResultSets.remove(resultSet);
        connection.closingResultSet(resultSet);
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
    
    /* Statement */

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        this.sql = sql;
        return executeQueryInternal(connection.compileExecutableStatement(sql), null, null);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        this.sql = sql;
        return executeUpdateInternal(connection.compileExecutableStatement(sql), null, null);
    }

    @Override
    public void close() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close(); // Which will call thru us to connection.
            currentResultSet = null;
        }
        if (generatedKeys != null) {
            generatedKeys.close();
            generatedKeys = null;
        }
        if (secondaryResultSets != null) {
            while (!secondaryResultSets.isEmpty()) {
                secondaryResultSets.get(0).close();
            }
            secondaryResultSets = null;
        }
        if (pendingResultSets != null) {
            while (!pendingResultSets.isEmpty())
                pendingResultSets.remove().close();
            pendingResultSets = null;
        }
        closed = true;
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
    }

    @Override
    public void cancel() throws SQLException {
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
    public void setCursorName(String name) throws SQLException {
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        this.sql = sql;
        return executeInternal(connection.compileExecutableStatement(sql), null, null);
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return currentUpdateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
            currentResultSet = null;
        }
        if (pendingResultSets == null)
            return false;
        currentResultSet = pendingResultSets.poll();
        return (currentResultSet != null);
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD)
            throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType()  throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public Connection getConnection()  throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return generatedKeys;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = sql;
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(autoGeneratedKeys)),
                                     null, null);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        this.sql = sql;
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnIndexes)),
                                     null, null);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        this.sql = sql;
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnNames)),
          null, null);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        this.sql = sql;
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(autoGeneratedKeys)),
          null, null);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        this.sql = sql;
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnIndexes)),
          null, null);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        this.sql = sql;
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnNames)),
          null, null);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

}
