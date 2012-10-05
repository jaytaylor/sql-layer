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

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.sql.server.ServerJavaValues;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.io.*;
import java.util.*;

public class JDBCPreparedStatement extends JDBCStatement implements PreparedStatement
{
    protected InternalStatement internalStatement;
    protected EmbeddedQueryContext context;
    protected final Values values = new Values();

    protected JDBCPreparedStatement(JDBCConnection connection, 
                                    InternalStatement internalStatement) {
        super(connection);
        this.internalStatement = internalStatement;
        context = new EmbeddedQueryContext(connection, this);
    }

    protected class Values extends ServerJavaValues {
        @Override
        protected ValueSource getValue(int parameterIndex) {
            return context.getValue(parameterIndex - 1);
        }

        @Override
        protected void setValue(int parameterIndex, ValueSource source, AkType akType) {
            context.setValue(parameterIndex - 1, source, akType);
        }

        @Override
        protected PValueSource getPValue(int parameterIndex) {
            return context.getPValue(parameterIndex - 1);
        }

        @Override
        protected void setPValue(int parameterIndex, PValueSource source) {
            context.setPValue(parameterIndex - 1, source);
        }

        @Override
        protected ResultSet toResultSet(int parameterIndex, Object resultSet) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected AkType getTargetType(int parameterIndex) {
            return internalStatement.getParameterMetaData().getParameter(parameterIndex).getAkType();
        }
    }

    /* PreparedStatement */

    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQueryInternal(internalStatement, context);
    }

    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdateInternal(internalStatement, context);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        try {
            values.setNull(parameterIndex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        try {
            values.setBoolean(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        try {
            values.setByte(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        try {
            values.setShort(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        try {
            values.setInt(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        try {
            values.setLong(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        try {
            values.setFloat(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        try {
            values.setDouble(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        try {
            values.setBigDecimal(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        try {
            values.setString(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        try {
            values.setBytes(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        try {
            values.setDate(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        try {
            values.setTime(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        try {
            values.setTimestamp(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setUnicodeStream(parameterIndex, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void clearParameters() throws SQLException {
        context.clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        try {
            values.setObject(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public boolean execute() throws SQLException {
        return executeInternal(internalStatement, context);
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex, reader, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        try {
            values.setRef(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        try {
            values.setBlob(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        try {
            values.setClob(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        try {
            values.setArray(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return internalStatement.getResultSetMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        try {
            values.setDate(parameterIndex, x, cal);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        try {
            values.setTime(parameterIndex, x, cal);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        try {
            values.setTimestamp(parameterIndex, x, cal);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNull (int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        try {
            values.setURL(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return internalStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        try {
            values.setRowId(parameterIndex, x);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        try {
            values.setNString(parameterIndex, value);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        try {
            values.setNCharacterStream(parameterIndex, value, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        try {
            values.setNClob(parameterIndex, value);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setClob(parameterIndex, reader, length);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        try {
            values.setBlob(parameterIndex, inputStream, length);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setNClob(parameterIndex, reader, length);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        try {
            values.setSQLXML(parameterIndex, xmlObject);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex, x, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex, reader, length);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            values.setAsciiStream(parameterIndex, x);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        try {
            values.setBinaryStream(parameterIndex, x);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setCharacterStream(parameterIndex, reader);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        try {
            values.setNCharacterStream(parameterIndex, value);
        }
        catch (IOException ex) {
            throw new JDBCException(ex);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setClob(parameterIndex, reader);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            values.setBlob(parameterIndex, inputStream);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            values.setNClob(parameterIndex, reader);
        }
        catch (RuntimeException ex) {
            JDBCException.throwUnwrapped(ex);
        }
    }
}
