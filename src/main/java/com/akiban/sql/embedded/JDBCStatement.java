/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.embedded;

import com.akiban.qp.operator.API;

import java.sql.*;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

public class JDBCStatement implements Statement
{
    protected final JDBCConnection connection;
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
    private Deque<ResultSet> pendingResultSets; // For instance, from stored procedure.

    protected JDBCStatement(JDBCConnection connection) {
        this.connection = connection;
    }

    public boolean executeInternal(ExecutableStatement stmt, EmbeddedQueryContext context) 
            throws SQLException {
        if (context == null) {
            if (stmt.getParameterMetaData() != null)
                throw new JDBCException("Statement requires parameters; must prepare");
            context = new EmbeddedQueryContext(this);
        }
        boolean hasResultSet = false;
        try {
            ExecuteResults results = stmt.execute(context);
            currentUpdateCount = results.getUpdateCount();
            if (results.getOperator() != null) {
                JDBCResultSet resultSet = new JDBCResultSet(this, stmt.getResultSetMetaData());
                boolean success = false;
                try {
                    // Create cursor and open it within transaction.
                    connection.openingResultSet(resultSet);
                    resultSet.open(API.cursor(results.getOperator(), context));
                    currentResultSet = resultSet;
                    success = true;
                }
                finally {
                    if (!success)
                        connection.closingResultSet(resultSet);
                }
                hasResultSet = true;
            }
            else if (results.getGeneratedKeys() != null) {
                // These are copied (to get update count) and do not need a transaction.
                JDBCResultSet resultSet = new JDBCResultSet(this, stmt.getResultSetMetaData());
                resultSet.open(results.getGeneratedKeys());
                generatedKeys = resultSet;
            }
            else if (results.getAdditionalResultSets() != null) {
                pendingResultSets = results.getAdditionalResultSets();
                hasResultSet = getMoreResults();
            }
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
        return hasResultSet;
    }

    public ResultSet executeQueryInternal(ExecutableStatement stmt, 
                                          EmbeddedQueryContext context) 
            throws SQLException {
        boolean hasResultSet = executeInternal(stmt, context);
        if (!hasResultSet) throw new JDBCException("Statement is not SELECT");
        return getResultSet();
    }

    public int executeUpdateInternal(ExecutableStatement stmt, 
                                     EmbeddedQueryContext context) 
            throws SQLException {
        boolean hasResultSet = executeInternal(stmt, context);
        if (hasResultSet) throw new JDBCException("Statement is SELECT");
        return getUpdateCount();
    }
    
    protected void addWarning(JDBCWarning warning) {
        if (warnings == null)
            warnings = warning;
        else
            warnings.setNextWarning(warning);
    }

    protected void openingResultSet(JDBCResultSet resultSet) {
        if (secondaryResultSets == null)
            secondaryResultSets = new ArrayList<ResultSet>();
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
        return executeQueryInternal(connection.compileExecutableStatement(sql), null);
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        return executeUpdateInternal(connection.compileExecutableStatement(sql), null);
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
                pendingResultSets.removeFirst().close();
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
        return executeInternal(connection.compileExecutableStatement(sql), null);
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
        currentResultSet = pendingResultSets.pollFirst();
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
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(autoGeneratedKeys)),
                                     null);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnIndexes)),
                                     null);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdateInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnNames)),
          null);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(autoGeneratedKeys)),
          null);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnIndexes)),
          null);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return executeInternal(
          connection.compileExecutableStatement(sql,
                                                ExecuteAutoGeneratedKeys.of(columnNames)),
          null);
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

    //@Override // JDK 1.7
    public void closeOnCompletion() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    //@Override // JDK 1.7
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
    }

}
