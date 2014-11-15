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
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.sql.server.ServerJavaValues;
import com.foundationdb.sql.server.ServerQueryContext;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.io.*;
import java.util.*;

public class JDBCPreparedStatement extends JDBCStatement implements PreparedStatement
{
    protected final ExecutableStatement executableStatement;
    protected final EmbeddedQueryContext context;
    protected final QueryBindings bindings;
    protected final Values values = new Values();

    protected JDBCPreparedStatement(JDBCConnection connection, String sql,
                                    ExecutableStatement executableStatement) {
        super(connection);
        this.sql = sql;
        this.executableStatement = executableStatement;
        context = new EmbeddedQueryContext(this);
        bindings = context.createBindings();
    }

    protected class Values extends ServerJavaValues {
        @Override
        protected int size() {
            return executableStatement.getParameterMetaData().getParameters().size();
        }

        @Override
        protected ServerQueryContext getContext() {
            return context;
        }

        @Override
        protected ValueSource getValue(int index) {
            return bindings.getValue(index);
        }

        @Override
        protected void setValue(int index, ValueSource source) {
            bindings.setValue(index, source);
        }


        @Override
        protected TInstance getType(int index) {
            return executableStatement.getParameterMetaData().getParameter(index + 1).getType();
        }

        @Override
        protected ResultSet toResultSet(int index, Object resultSet) {
            throw new UnsupportedOperationException();
        }
    }

    // TODO: Will need a separate interface for these when class loader
    // isolated implementation classes from stored procedures.

    /** Return a <code>PreparedStatement</code> with the same SQL but
     * separate results.
     */
    public JDBCPreparedStatement duplicate() {
        return new JDBCPreparedStatement(connection, sql, executableStatement);
    }

    /** Return the estimated number of rows that will be returned or
     * <code>-1</code> if unknown.
     */
    public long getEstimatedRowCount() throws SQLException {
        return executableStatement.getEstimatedRowCount();
    }

    /* PreparedStatement */

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQueryInternal(executableStatement, context, bindings);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdateInternal(executableStatement, context, bindings);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            values.setNull(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            values.setBoolean(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            values.setByte(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            values.setShort(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            values.setInt(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            values.setLong(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            values.setFloat(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            values.setDouble(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            values.setBigDecimal(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            values.setString(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        try {
            values.setBytes(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            values.setDate(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            values.setTime(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            values.setTimestamp(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex - 1, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Deprecated
    @SuppressWarnings("deprecation")
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setUnicodeStream(parameterIndex - 1, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex - 1, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        bindings.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            values.setObject(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return executeInternal(executableStatement, context, bindings);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex - 1, reader, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            values.setRef(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            values.setBlob(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            values.setClob(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            values.setArray(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return executableStatement.getResultSetMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            values.setDate(parameterIndex - 1, x, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            values.setTime(parameterIndex - 1, x, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            values.setTimestamp(parameterIndex - 1, x, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNull (int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            values.setURL(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return executableStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            values.setRowId(parameterIndex - 1, x);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            values.setNString(parameterIndex - 1, value);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            values.setNCharacterStream(parameterIndex - 1, value, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            values.setNClob(parameterIndex - 1, value);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setClob(parameterIndex - 1, reader, length);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            values.setBlob(parameterIndex - 1, inputStream, length);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setNClob(parameterIndex - 1, reader, length);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            values.setSQLXML(parameterIndex - 1, xmlObject);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex - 1, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex - 1, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex - 1, reader, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex - 1, x);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex - 1, x);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex - 1, reader);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            values.setNCharacterStream(parameterIndex - 1, value);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setClob(parameterIndex - 1, reader);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            values.setBlob(parameterIndex - 1, inputStream);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setNClob(parameterIndex - 1, reader);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }
}
