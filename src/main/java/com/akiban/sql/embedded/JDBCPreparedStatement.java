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

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.WrappingByteSource;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.io.*;
import java.util.*;

public class JDBCPreparedStatement extends JDBCStatement implements PreparedStatement
{
    private InternalStatement internalStatement;
    private JDBCQueryContext context;
    private FromObjectValueSource objectSource;

    protected JDBCPreparedStatement(JDBCConnection connection, 
                                    InternalStatement internalStatement) {
        super(connection);
        this.internalStatement = internalStatement;
        context = new JDBCQueryContext(connection);
    }

    protected void setParameter(int parameterIndex, Object value, AkType sourceType) throws SQLException {
        AkType targetType = internalStatement.getParameterMetaData().getParameter(parameterIndex).getAkType();
        try {
            if (Types3Switch.ON) {
                if (sourceType == null)
                    sourceType = targetType;
                PValueSource source = PValueSources.fromObject(value, sourceType).value();
                context.setPValue(parameterIndex - 1, source);
            }
            else {
                if (objectSource == null)
                    objectSource = new FromObjectValueSource();
                if (sourceType != null)
                    objectSource.setExplicitly(value, sourceType);
                else
                    objectSource.setReflectively(value);
                context.setValue(parameterIndex - 1, objectSource, targetType);
            }
        }
        catch (InvalidOperationException ex) {
            throw new SQLException(ex);
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
        setParameter(parameterIndex, null, AkType.NULL);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x, AkType.BOOL);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, (int)x, AkType.INT);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, (int)x, AkType.INT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, x, AkType.INT);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, x, AkType.LONG);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, x, AkType.FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, x, AkType.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x, AkType.DECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x, AkType.VARCHAR);
    }

    @Override
    public void setBytes(int parameterIndex, byte x[]) throws SQLException {
        setParameter(parameterIndex, new WrappingByteSource(x), AkType.VARBINARY);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        // TODO: Aren't there system routines to do this someplace?
        DateTime dt = new DateTime(x, DateTimeZone.getDefault());
        long encoded = dt.getYear() * 512 + dt.getMonthOfYear() * 32 + dt.getDayOfMonth();
        setParameter(parameterIndex, encoded, AkType.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        DateTime dt = new DateTime(x, DateTimeZone.getDefault());
        long encoded = dt.getHourOfDay() * 10000 + dt.getMinuteOfHour() * 100 + dt.getSecondOfMinute();
        setParameter(parameterIndex, encoded, AkType.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, x.getTime() / 1000, AkType.TIMESTAMP);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String value;
        try {
            byte[] b = new byte[length];
            int l = x.read(b);
            value = new String(b, 0, l, "ASCII");
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARCHAR);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        String value;
        try {
            byte[] b = new byte[length];
            int l = x.read(b);
            value = new String(b, 0, l, "UTF-8");
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARCHAR);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        WrappingByteSource value;
        try {
            byte[] b = new byte[length];
            int l = x.read(b);
            value = new WrappingByteSource().wrap(b, 0, l);
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARBINARY);
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
        setParameter(parameterIndex, x, null);
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
        String value;
        try {
            char[] c = new char[length];
            int l = reader.read(c);
            value = new String(c, 0, l);
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARCHAR);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return internalStatement.getResultSetMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        cal.setTime(x);
        DateTime dt = new DateTime(cal);
        long encoded = dt.getYear() * 512 + dt.getMonthOfYear() * 32 + dt.getDayOfMonth();
        setParameter(parameterIndex, encoded, AkType.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        cal.setTime(x);
        DateTime dt = new DateTime(cal);
        long encoded = dt.getHourOfDay() * 10000 + dt.getMinuteOfHour() * 100 + dt.getSecondOfMinute();
        setParameter(parameterIndex, encoded, AkType.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull (int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return internalStatement.getParameterMetaData();
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex, x, (int)length);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex, x, (int)length);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (int)length);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        String value;
        try {
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            while (true) {
                int len = x.read(buf);
                if (len < 0) break;
                ostr.write(buf, 0, len);
            }
            value = new String(ostr.toByteArray(), "ASCII");
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARCHAR);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        WrappingByteSource value;
        try {
            ByteArrayOutputStream ostr = new ByteArrayOutputStream();
            byte[] buf = new byte[1024];
            while (true) {
                int len = x.read(buf);
                if (len < 0) break;
                ostr.write(buf, 0, len);
            }
            value = new WrappingByteSource(ostr.toByteArray());
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARBINARY);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        String value;
        try {
            StringWriter ostr = new StringWriter();
            char[] buf = new char[1024];
            while (true) {
                int len = reader.read(buf);
                if (len < 0) break;
                ostr.write(buf, 0, len);
            }
            value = ostr.toString();
        }
        catch (IOException ex) {
            throw new SQLException(ex);
        }
        setParameter(parameterIndex, value, AkType.VARCHAR);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex, value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }
}
