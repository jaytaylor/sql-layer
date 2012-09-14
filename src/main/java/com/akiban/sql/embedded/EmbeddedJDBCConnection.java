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

import com.akiban.sql.server.ServerServiceRequirements;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.Executor;

public class EmbeddedJDBCConnection implements Connection {
    private final ServerServiceRequirements reqs;
    private final Properties info;
    private boolean closed, autoCommit, readOnly;
    private SQLWarning warnings;
    private Properties clientInfo = new Properties();
    private String schema;
    
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedJDBCConnection.class);

    public EmbeddedJDBCConnection(ServerServiceRequirements reqs, Properties info) {
        this.reqs = reqs;
        this.info = info;
    }

    /* Wrapper */

    public <T> T unwrap(java.lang.Class<T> iface) throws SQLException {
        throw new SQLException("Not supported");
    }
    
    public boolean isWrapperFor(java.lang.Class<?> iface) throws java.sql.SQLException {
        return false;
    }

    /* Connection */

    public Statement createStatement() throws SQLException {
        return null;
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return null;
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return null;
    }

    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    public boolean getAutoCommit() throws SQLException {
        return autoCommit;
    }

    public void commit() throws SQLException {
    }

    public void rollback() throws SQLException {
    }

    public void close() throws SQLException {
        this.closed = true;
    }

    public boolean isClosed() throws SQLException {
        return closed;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return null;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
    }

    public boolean isReadOnly() throws SQLException {
        return readOnly;
    }

    public void setCatalog(String catalog) throws SQLException {
    }

    public String getCatalog() throws SQLException {
        return null;
    }

    public void setTransactionIsolation(int level) throws SQLException {
        if (level != TRANSACTION_SERIALIZABLE)
            throw new SQLException("Only TRANSACTION_SERIALIZABLE supported");
    }

    public int getTransactionIsolation() throws SQLException {
        return TRANSACTION_SERIALIZABLE;
    }

    public SQLWarning getWarnings() throws SQLException {
        return warnings;
    }

    public void clearWarnings() throws SQLException {
        warnings = null;
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return createStatement();
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY))
            throw new SQLFeatureNotSupportedException();
        return prepareCall(sql);
    }

    public Map<String,Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /**
     * Installs the given <code>TypeMap</code> object as the type map for
     * this <code>Connection</code> object.  The type map will be used for the
     * custom mapping of SQL structured types and distinct types.
     *<p>
     * You must set the the values for the <code>TypeMap</code> prior to
     * callng <code>setMap</code> as a JDBC driver may create an internal copy
     * of the <code>TypeMap</code>:
     * <p>
     * <pre>
     *      Map myMap&lt;String,Class&lt;?&gt;&gt; = new HashMap&lt;String,Class&lt;?&gt;&gt;();
     *      myMap.put("mySchemaName.ATHLETES", Athletes.class);
     *      con.setTypeMap(myMap);
     * </pre>
     * @param map the <code>java.util.Map</code> object to install
     *        as the replacement for this <code>Connection</code>
     *        object's default type map
     * @exception SQLException if a database access error occurs, this
     * method is called on a closed connection or
     *        the given parameter is not a <code>java.util.Map</code>
     *        object
     * @exception SQLFeatureNotSupportedException if the JDBC driver does not support
     * this method
     * @since 1.2
     * @see #getTypeMap
     */
    public void setTypeMap(java.util.Map<String,Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
    
    //--------------------------JDBC 3.0-----------------------------


    public void setHoldability(int holdability) throws SQLException {
        if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT)
            throw new SQLFeatureNotSupportedException();
    }

    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency,
                                     int resultSetHoldability) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return createStatement();
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType,
                                              int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return prepareStatement(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType,
                                         int resultSetConcurrency,
                                         int resultSetHoldability) throws SQLException {
        if ((resultSetType != ResultSet.TYPE_FORWARD_ONLY) ||
            (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) ||
            (resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT))
            throw new SQLFeatureNotSupportedException();
        return prepareCall(sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
            throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS)
            throw new SQLFeatureNotSupportedException();
        return prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int columnIndexes[])
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public PreparedStatement prepareStatement(String sql, String columnNames[])
            throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public boolean isValid(int timeout) throws SQLException {
        return !closed;
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.put(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        clientInfo.putAll(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    public String getSchema() throws SQLException {
        return schema;
    }

    public void abort(Executor executor) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }


    public int getNetworkTimeout() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
