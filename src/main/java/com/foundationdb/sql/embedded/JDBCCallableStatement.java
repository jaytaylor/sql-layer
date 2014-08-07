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

import com.foundationdb.ais.model.Routine;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.util.AkibanAppender;

import java.sql.*;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.util.Calendar;
import java.util.Map;

public class JDBCCallableStatement extends JDBCPreparedStatement implements CallableStatement
{
    protected JDBCCallableStatement(JDBCConnection connection, String sql,
                                    ExecutableStatement executableStatement) {
        super(connection, sql, executableStatement);
    }

    protected int findParameter(String parameterName) throws SQLException {
        JDBCParameterMetaData metaData = executableStatement.getParameterMetaData();
        for (int i = 1; i <= metaData.getParameterCount(); i++) {
            if (parameterName.equalsIgnoreCase(metaData.getParameter(i).getName())) {
                return i;
            }
        }
        throw new JDBCException("Parameter not found: " + parameterName);
    }

    public Routine getRoutine() {
        return ((ExecutableCallStatement)
                executableStatement).getInvocation().getRoutine();
    }

    public void formatAsJson(int parameterIndex, AkibanAppender appender, FormatOptions options) {
        values.formatAsJson(parameterIndex - 1, appender, options);
    }

    /* CallableStatement */

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType) throws SQLException {
        // No work needed, can get out as whatever type works.
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public boolean wasNull() throws SQLException {
        return values.wasNull();
    }

    @Override
    public String getString(int parameterIndex) throws SQLException {
        try {
            return values.getString(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public boolean getBoolean(int parameterIndex) throws SQLException {
        try {
            return values.getBoolean(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public byte getByte(int parameterIndex) throws SQLException {
        try {
            return values.getByte(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public short getShort(int parameterIndex) throws SQLException {
        try {
            return values.getShort(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public int getInt(int parameterIndex) throws SQLException {
        try {
            return values.getInt(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public long getLong(int parameterIndex) throws SQLException {
        try {
            return values.getLong(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public float getFloat(int parameterIndex) throws SQLException {
        try {
            return values.getFloat(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public double getDouble(int parameterIndex) throws SQLException {
        try {
            return values.getDouble(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Deprecated // as in parent class
    @SuppressWarnings("deprecation")
    @Override
    public BigDecimal getBigDecimal(int parameterIndex, int scale) throws SQLException {
        return getBigDecimal(parameterIndex).setScale(scale);
    }

    @Override
    public byte[] getBytes(int parameterIndex) throws SQLException {
        try {
            return values.getBytes(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Date getDate(int parameterIndex) throws SQLException {
        try {
            return values.getDate(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Time getTime(int parameterIndex) throws SQLException {
        try {
            return values.getTime(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex) throws SQLException {
        try {
            return values.getTimestamp(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Object getObject(int parameterIndex) throws SQLException {
        try {
            return values.getObject(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
        try {
            return values.getBigDecimal(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Object getObject(int parameterIndex, Map<String,Class<?>> map) throws SQLException {
        return getObject(parameterIndex);
    }

    @Override
    public Ref getRef(int parameterIndex) throws SQLException {
        try {
            return values.getRef(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Blob getBlob(int parameterIndex) throws SQLException {
        try {
            return values.getBlob(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Clob getClob(int parameterIndex) throws SQLException {
        try {
            return values.getClob(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Array getArray(int parameterIndex) throws SQLException {
        try {
            return values.getArray(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return values.getDate(parameterIndex - 1, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return values.getTime(parameterIndex - 1, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Timestamp getTimestamp(int parameterIndex, Calendar cal) throws SQLException {
        try {
            return values.getTimestamp(parameterIndex - 1, cal);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType) throws SQLException {
        registerOutParameter(findParameter(parameterName), sqlType);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, int scale) throws SQLException {
        registerOutParameter(findParameter(parameterName), sqlType, scale);
    }

    @Override
    public void registerOutParameter(String parameterName, int sqlType, String typeName) throws SQLException {
        registerOutParameter(findParameter(parameterName), sqlType, typeName);
    }

    @Override
    public URL getURL(int parameterIndex) throws SQLException {
        try {
            return values.getURL(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public void setURL(String parameterName, URL val) throws SQLException {
        setURL(findParameter(parameterName), val);
    }

    @Override
    public void setNull(String parameterName, int sqlType) throws SQLException {
        setNull(findParameter(parameterName), sqlType);
    }

    @Override
    public void setBoolean(String parameterName, boolean x) throws SQLException {
        setBoolean(findParameter(parameterName), x);
    }

    @Override
    public void setByte(String parameterName, byte x) throws SQLException {
        setByte(findParameter(parameterName), x);
    }

    @Override
    public void setShort(String parameterName, short x) throws SQLException {
        setShort(findParameter(parameterName), x);
    }

    @Override
    public void setInt(String parameterName, int x) throws SQLException {
        setInt(findParameter(parameterName), x);
    }

    @Override
    public void setLong(String parameterName, long x) throws SQLException {
        setLong(findParameter(parameterName), x);
    }

    @Override
    public void setFloat(String parameterName, float x) throws SQLException {
        setFloat(findParameter(parameterName), x);
    }

    @Override
    public void setDouble(String parameterName, double x) throws SQLException {
        setDouble(findParameter(parameterName), x);
    }

    @Override
    public void setBigDecimal(String parameterName, BigDecimal x) throws SQLException {
        setBigDecimal(findParameter(parameterName), x);
    }

    @Override
    public void setString(String parameterName, String x) throws SQLException {
        setString(findParameter(parameterName), x);
    }

    @Override
    public void setBytes(String parameterName, byte[] x) throws SQLException {
        setBytes(findParameter(parameterName), x);
    }

    @Override
    public void setDate(String parameterName, Date x) throws SQLException {
        setDate(findParameter(parameterName), x);
    }

    @Override
    public void setTime(String parameterName, Time x) throws SQLException {
        setTime(findParameter(parameterName), x);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x) throws SQLException {
        setTimestamp(findParameter(parameterName), x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, int length) throws SQLException {
        setAsciiStream(findParameter(parameterName), x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, int length) throws SQLException {
        setBinaryStream(findParameter(parameterName), x, length);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType, int scale) throws SQLException {
        setObject(findParameter(parameterName), x, targetSqlType, scale);
    }

    @Override
    public void setObject(String parameterName, Object x, int targetSqlType) throws SQLException {
        setObject(findParameter(parameterName), x, targetSqlType);
    }

    @Override
    public void setObject(String parameterName, Object x) throws SQLException {
        setObject(findParameter(parameterName), x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, int length) throws SQLException {
        setCharacterStream(findParameter(parameterName), reader, length);
    }

    @Override
    public void setDate(String parameterName, Date x, Calendar cal) throws SQLException {
        setDate(findParameter(parameterName), x, cal);
    }

    @Override
    public void setTime(String parameterName, Time x, Calendar cal) throws SQLException {
        setTime(findParameter(parameterName), x, cal);
    }

    @Override
    public void setTimestamp(String parameterName, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(findParameter(parameterName), x, cal);
    }

    @Override
    public void setNull(String parameterName, int sqlType, String typeName) throws SQLException {
        setNull(findParameter(parameterName), sqlType, typeName);
    }

    @Override
    public String getString(String parameterName) throws SQLException {
        return getString(findParameter(parameterName));
    }

    @Override
    public boolean getBoolean(String parameterName) throws SQLException {
        return getBoolean(findParameter(parameterName));
    }

    @Override
    public byte getByte(String parameterName) throws SQLException {
        return getByte(findParameter(parameterName));
    }

    @Override
    public short getShort(String parameterName) throws SQLException {
        return getShort(findParameter(parameterName));
    }

    @Override
    public int getInt(String parameterName) throws SQLException {
        return getInt(findParameter(parameterName));
    }

    @Override
    public long getLong(String parameterName) throws SQLException {
        return getLong(findParameter(parameterName));
    }

    @Override
    public float getFloat(String parameterName) throws SQLException {
        return getFloat(findParameter(parameterName));
    }

    @Override
    public double getDouble(String parameterName) throws SQLException {
        return getDouble(findParameter(parameterName));
    }

    @Override
    public byte[] getBytes(String parameterName) throws SQLException {
        return getBytes(findParameter(parameterName));
    }

    @Override
    public Date getDate(String parameterName) throws SQLException {
        return getDate(findParameter(parameterName));
    }

    @Override
    public Time getTime(String parameterName) throws SQLException {
        return getTime(findParameter(parameterName));
    }

    @Override
    public Timestamp getTimestamp(String parameterName) throws SQLException {
        return getTimestamp(findParameter(parameterName));
    }

    @Override
    public Object getObject(String parameterName) throws SQLException {
        return getObject(findParameter(parameterName));
    }

    @Override
    public BigDecimal getBigDecimal(String parameterName) throws SQLException {
        return getBigDecimal(findParameter(parameterName));
    }

    @Override
    public Object getObject(String parameterName, Map<String,Class<?>> map) throws SQLException {
        return getObject(findParameter(parameterName), map);
    }

    @Override
    public Ref getRef(String parameterName) throws SQLException {
        return getRef(findParameter(parameterName));
    }

    @Override
    public Blob getBlob(String parameterName) throws SQLException {
        return getBlob(findParameter(parameterName));
    }

    @Override
    public Clob getClob(String parameterName) throws SQLException {
        return getClob(findParameter(parameterName));
    }

    @Override
    public Array getArray(String parameterName) throws SQLException {
        return getArray(findParameter(parameterName));
    }

    @Override
    public Date getDate(String parameterName, Calendar cal) throws SQLException {
        return getDate(findParameter(parameterName), cal);
    }

    @Override
    public Time getTime(String parameterName, Calendar cal) throws SQLException {
        return getTime(findParameter(parameterName), cal);
    }

    @Override
    public Timestamp getTimestamp(String parameterName, Calendar cal) throws SQLException {
        return getTimestamp(findParameter(parameterName), cal);
    }

    @Override
    public URL getURL(String parameterName) throws SQLException {
        return getURL(findParameter(parameterName));
    }

    @Override
    public RowId getRowId(int parameterIndex) throws SQLException {
        try {
            return values.getRowId(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public RowId getRowId(String parameterName) throws SQLException {
        return getRowId(findParameter(parameterName));
    }

    @Override
    public void setRowId(String parameterName, RowId x) throws SQLException {
        setRowId(findParameter(parameterName), x);
    }

    @Override
    public void setNString(String parameterName, String value) throws SQLException {
        setNString(findParameter(parameterName), value);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value, long length) throws SQLException {
        setNCharacterStream(findParameter(parameterName), value, length);
    }

    @Override
    public void setNClob(String parameterName, NClob value) throws SQLException {
        setNClob(findParameter(parameterName), value);
    }

    @Override
    public void setClob(String parameterName, Reader reader, long length) throws SQLException {
        setClob(findParameter(parameterName), reader, length);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream, long length) throws SQLException {
        setBlob(findParameter(parameterName), inputStream, length);
    }

    @Override
    public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
        setNClob(findParameter(parameterName), reader, length);
    }

    @Override
    public NClob getNClob(int parameterIndex) throws SQLException {
        try {
            return values.getNClob(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public NClob getNClob(String parameterName) throws SQLException {
        return getNClob(findParameter(parameterName));
    }

    @Override
    public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
        setSQLXML(findParameter(parameterName), xmlObject);
    }

    @Override
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        try {
            return values.getSQLXML(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public SQLXML getSQLXML(String parameterName) throws SQLException {
        return getSQLXML(findParameter(parameterName));
    }

    @Override
    public String getNString(int parameterIndex) throws SQLException {
        try {
            return values.getNString(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public String getNString(String parameterName) throws SQLException {
        return getNString(findParameter(parameterName));
    }

    @Override
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        try {
            return values.getNCharacterStream(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Reader getNCharacterStream(String parameterName) throws SQLException {
        return getNCharacterStream(findParameter(parameterName));
    }

    @Override
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        try {
            return values.getCharacterStream(parameterIndex - 1);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public Reader getCharacterStream(String parameterName) throws SQLException {
        return getCharacterStream(findParameter(parameterName));
    }

    @Override
    public void setBlob(String parameterName, Blob x) throws SQLException {
        setBlob(findParameter(parameterName), x);
    }

    @Override
    public void setClob(String parameterName, Clob x) throws SQLException {
        setClob(findParameter(parameterName), x);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
        setAsciiStream(findParameter(parameterName), x, length);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
        setBinaryStream(findParameter(parameterName), x, length);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
        setCharacterStream(findParameter(parameterName), reader, length);
    }

    @Override
    public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
        setAsciiStream(findParameter(parameterName), x);
    }

    @Override
    public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
        setBinaryStream(findParameter(parameterName), x);
    }

    @Override
    public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
        setCharacterStream(findParameter(parameterName), reader);
    }

    @Override
    public void setNCharacterStream(String parameterName, Reader value) throws SQLException {
        setNCharacterStream(findParameter(parameterName), value);
    }

    @Override
    public void setClob(String parameterName, Reader reader) throws SQLException {
        setClob(findParameter(parameterName), reader);
    }

    @Override
    public void setBlob(String parameterName, InputStream inputStream) throws SQLException {
        setBlob(findParameter(parameterName), inputStream);
    }

    @Override
    public void setNClob(String parameterName, Reader reader) throws SQLException {
        setNClob(findParameter(parameterName), reader);
    }

    @Override
    public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException {
        try {
            return (T)values.getObject(parameterIndex - 1, type);
        }
        catch (RuntimeException ex) {
            throw JDBCException.throwUnwrapped(ex);
        }
    }

    @Override
    public <T> T getObject(String parameterName, Class<T> type) throws SQLException {
        return getObject(findParameter(parameterName), type);
    }
}
