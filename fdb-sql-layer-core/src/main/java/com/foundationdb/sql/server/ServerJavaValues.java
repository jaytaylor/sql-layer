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

package com.foundationdb.sql.server;

import com.foundationdb.server.error.NoSuchCastException;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.FormatOptions;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;
import com.foundationdb.util.WrappingByteSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.Collections;

/** Make something like an array of typed values (for instance, a
 * <code>Row</code> or a <code>QueryContext</code>) accessible using
 * standard Java types, includings ones from the <code>java.sql</code>
 * package.
 * 
 * API like <code>ResultSet</code> or <code>PreparedStatement</code>.
 */
public abstract class ServerJavaValues
{
    public static final int RETURN_VALUE_INDEX = -1;

    protected abstract int size();
    protected abstract ServerQueryContext getContext();
    protected abstract ValueSource getValue(int index);
    // TODO ensure this is never null
    protected abstract TInstance getType(int index);
    protected abstract void setValue(int index, ValueSource source);
    protected abstract ResultSet toResultSet(int index, Object resultSet);

    private boolean wasNull;
    private CachedCast[] cachedCasts;

    protected ValueSource value(int index) {
        ValueSource value = getValue(index);
        wasNull = value.isNull();
        return value;
    }

    protected TypesTranslator getTypesTranslator() {
        return getContext().getTypesTranslator();
    }

    protected static class CachedCast {
        TClass targetClass;
        TCast tcast;
        TExecutionContext tcontext;
        Value target;

        protected CachedCast(TInstance sourceInstance, TClass targetClass, 
                             ServerQueryContext context) {
            this.targetClass = targetClass;
            TInstance targetInstance = targetClass.instance(sourceInstance == null || sourceInstance.nullability());
            tcast = context.getServer().typesRegistryService().getCastsResolver()
                .cast(sourceInstance, targetInstance);
            if (tcast == null)
                throw new NoSuchCastException(sourceInstance, targetInstance);
            tcontext = new TExecutionContext(Collections.singletonList(sourceInstance),
                                             targetInstance,
                                             context);
            target = new Value(targetInstance);
        }

        protected boolean matches(TClass required) {
            return required.equals(targetClass);
        }

        protected ValueSource apply(ValueSource value) {
            tcast.evaluate(tcontext, value, target);
            return target;
        }
    }

    /** Cast as necessary to <code>required</code>.
     * A cache is maintained for each index with the last class, on
     * the assumption the caller will be applying the same
     * <code>getXxx</code> / <code>setXxx</code> to the same field each time.
     */
    protected ValueSource cachedCast(int index, ValueSource value, int jdbcType) {
        return cachedCast(index, value, getType(index), jdbcType);
    }

    protected ValueSource cachedCast(int index, ValueSource source, TInstance sourceType, int jdbcType) {
        if (jdbcType == getTypesTranslator().jdbcType(sourceType))
            return source;
        return cachedCast(index, source, sourceType,
                          getTypesTranslator().typeClassForJDBCType(jdbcType));
    }

    protected ValueSource cachedCast(int index, ValueSource value, TClass required) {
        return cachedCast(index, value, getType(index), required);
    }

    protected ValueSource cachedCast(int index, ValueSource source, TInstance sourceType, TClass required) {
        if (required.equals(sourceType.typeClass()))
            return source;      // Already of the required class.
        // Leave room for return value (index does not matter -- only used here).
        if (cachedCasts == null)
            cachedCasts = new CachedCast[size() + 1];
        if (index == RETURN_VALUE_INDEX)
            index = cachedCasts.length - 1;
        CachedCast cast = cachedCasts[index];
        if ((cast == null) || !cast.matches(required)) {
            cast = new CachedCast(sourceType, required, getContext());
            cachedCasts[index] = cast;
        }
        return cast.apply(source);
    }

    protected TInstance jdbcInstance(int jdbcType) {
        return getTypesTranslator().typeClassForJDBCType(jdbcType).instance(true);
    }

    protected void setValue(int index, Object value, TInstance sourceType) {
        TInstance targetType = this.getType(index);
        if (sourceType == null) {
            sourceType = targetType;
        }
        ValueSource source = ValueSources.valuefromObject(value, sourceType);
        if (targetType != null)
            source = cachedCast(index, source, sourceType, targetType.typeClass());
        setValue(index, source);
    }

    public boolean wasNull() {
        return wasNull;
    }

    public String getString(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else
            return cachedCast(index, value, Types.VARCHAR).getString();
    }

    public String getNString(int index) {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return false;
        else
            return cachedCast(index, value, Types.BOOLEAN).getBoolean();
    }

    public byte getByte(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0;
        else
            return (byte)getTypesTranslator()
                .getIntegerValue(cachedCast(index, value, Types.TINYINT));
    }

    public short getShort(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0;
        else
            return (short)getTypesTranslator()
                .getIntegerValue(cachedCast(index, value, Types.SMALLINT));
    }

    public int getInt(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0;
        else
            return (int)getTypesTranslator()
                .getIntegerValue(cachedCast(index, value, Types.INTEGER));
    }

    public long getLong(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0;
        else
            return getTypesTranslator()
                .getIntegerValue(cachedCast(index, value, Types.BIGINT));
    }

    public float getFloat(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0.0f;
        else
            return cachedCast(index, value, Types.FLOAT).getFloat();
    }

    public double getDouble(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return 0.0;
        else
            return cachedCast(index, value, Types.DOUBLE).getDouble();
    }

    public BigDecimal getBigDecimal(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else
            return getTypesTranslator().getDecimalValue(cachedCast(index, value, Types.DECIMAL));
    }

    public byte[] getBytes(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else
            return cachedCast(index, value, Types.VARBINARY).getBytes();
    }

    public Date getDate(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else
            return new Date(getTypesTranslator().getTimestampMillisValue(value));
    }

    public Time getTime(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else {
            return new Time(getTypesTranslator().getTimestampMillisValue(value));
        }
    }

    public Timestamp getTimestamp(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else {
            Timestamp result = new Timestamp(getTypesTranslator().getTimestampMillisValue(value));
            result.setNanos(getTypesTranslator().getTimestampNanosValue(value));
            return result;
        }
    }

    public Date getDate(int index, Calendar cal) {
        return getDate(index);
    }

    public Time getTime(int index, Calendar cal) {
        return getTime(index);
    }

    public Timestamp getTimestamp(int index, Calendar cal) {
        return getTimestamp(index);
    }

    public ResultSet getResultSet(int index) {
        ValueSource value = value(index);
        if (wasNull)
            return null;
        else
            return toResultSet(index, value.getObject());
    }

    public Object getObject(int index) {
        return getObject(index, getTypesTranslator().jdbcClass(getType(index)));
    }

    public Object getObject(int index, Class<?> type) {
        if (type == String.class)
            return getString(index);
        else if (type == BigDecimal.class)
            return getBigDecimal(index);
        else if ((type == Boolean.class) || (type == Boolean.TYPE)) {
            boolean value = getBoolean(index);
            return wasNull() ? null : Boolean.valueOf(value);
        }
        else if ((type == Byte.class) || (type == Byte.TYPE)) {
            byte value = getByte(index);
            return wasNull() ? null : Byte.valueOf(value);
        }
        else if ((type == Short.class) || (type == Short.TYPE)) {
            short value = getShort(index);
            return wasNull() ? null : Short.valueOf(value);
        }
        else if ((type == Integer.class) || (type == Integer.TYPE)) {
            int value = getInt(index);
            return wasNull() ? null : Integer.valueOf(value);
        }
        else if ((type == Long.class) || (type == Long.TYPE)) {
            long value = getLong(index);
            return wasNull() ? null : Long.valueOf(value);
        }
        else if ((type == Float.class) || (type == Float.TYPE)) {
            float value = getFloat(index);
            return wasNull() ? null : Float.valueOf(value);
        }
        else if ((type == Double.class) || (type == Double.TYPE)) {
            double value = getDouble(index);
            return wasNull() ? null : Double.valueOf(value);
        }
        else if (type == byte[].class)
            return getBytes(index);
        else if (type == Date.class)
            return getDate(index);
        else if (type == Time.class)
            return getTime(index);
        else if (type == Timestamp.class)
            return getTimestamp(index);
        else if (type == ResultSet.class)
            return getResultSet(index);
        else if (type == Array.class)
            return getArray(index);
        else if (type == Blob.class)
            return getBlob(index);
        else if (type == Clob.class)
            return getClob(index);
        else if (type == Ref.class)
            return getRef(index);
        else if (type == URL.class)
            return getURL(index);
        else if (type == RowId.class)
            return getRowId(index);
        else if (type == NClob.class)
            return getNClob(index);
        else if (type == SQLXML.class)
            return getSQLXML(index);
        else 
            throw new UnsupportedOperationException("Unsupported type " + type);
    }

    public InputStream getAsciiStream(int index) {
        throw new UnsupportedOperationException();
    }

    public InputStream getUnicodeStream(int index) {
        throw new UnsupportedOperationException();
    }

    public Reader getCharacterStream(int index) {
        throw new UnsupportedOperationException();
    }

    public Reader getNCharacterStream(int index) {
        throw new UnsupportedOperationException();
    }

    public InputStream getBinaryStream(int index) {
        throw new UnsupportedOperationException();
    }

    public Ref getRef(int index) {
        throw new UnsupportedOperationException();
    }

    public Blob getBlob(int index) {
        throw new UnsupportedOperationException();
    }

    public Clob getClob(int index) {
        throw new UnsupportedOperationException();
    }

    public NClob getNClob(int index) {
        throw new UnsupportedOperationException();
    }

    public SQLXML getSQLXML(int index) {
        throw new UnsupportedOperationException();
    }

    public RowId getRowId(int index) {
        throw new UnsupportedOperationException();
    }

    public Array getArray(int index) {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int index) {
        throw new UnsupportedOperationException();
    }

    public void setNull(int index) {
        setValue(index, (Object)null, jdbcInstance(Types.INTEGER));
    }

    public void setBoolean(int index, boolean x) {
        setValue(index, x, jdbcInstance(Types.BOOLEAN));
    }

    public void setByte(int index, byte x) {
        setValue(index, (int)x, jdbcInstance(Types.TINYINT));
    }

    public void setShort(int index, short x) {
        setValue(index, (int)x, jdbcInstance(Types.SMALLINT)); 
    }

    public void setInt(int index, int x) {
        setValue(index, x, jdbcInstance(Types.INTEGER));
    }

    public void setLong(int index, long x) {
        setValue(index, x, jdbcInstance(Types.BIGINT));
    }

    public void setFloat(int index, float x) {
        setValue(index, x, jdbcInstance(Types.FLOAT));
    }

    public void setDouble(int index, double x) {
        setValue(index, x, jdbcInstance(Types.DOUBLE));
    }

    public void setBigDecimal(int index, BigDecimal x) {
        setValue(index, x, jdbcInstance(Types.DECIMAL));
    }

    public void setString(int index, String x) {
        setValue(index, x, jdbcInstance(Types.VARCHAR));
    }

    public void setBytes(int index, byte x[]) {
        setValue(index, new WrappingByteSource(x), jdbcInstance(Types.VARBINARY));
    }

    public void setDate(int index, Date x) {
        Value value = new Value(jdbcInstance(Types.DATE));
        getTypesTranslator().setTimestampMillisValue(value, x.getTime(), 0);
        setValue(index, value);
    }

    public void setTime(int index, Time x) {
        Value value = new Value(jdbcInstance(Types.TIME));
        getTypesTranslator().setTimestampMillisValue(value, x.getTime(), 0);
        setValue(index, value);
    }

    public void setTimestamp(int index, Timestamp x) {
        Value value = new Value(jdbcInstance(Types.TIMESTAMP));
        getTypesTranslator().setTimestampMillisValue(value, x.getTime(), x.getNanos());
        setValue(index, value);
    }

    public void setDate(int index, Date x, Calendar cal) {
        setDate(index, x);
    }

    public void setTime(int index, Time x, Calendar cal) {
        setTime(index, x);
    }

    public void setTimestamp(int index, Timestamp x, Calendar cal) {
        setTimestamp(index, x);
    }

    public void setObject(int index, Object x) {
        if (x == null)
            setNull(index);
        else if (x instanceof String)
            setString(index, (String)x);
        else if (x instanceof BigDecimal)
            setBigDecimal(index, (BigDecimal)x);
        else if (x instanceof Boolean)
            setBoolean(index, (Boolean)x);
        else if (x instanceof Byte)
            setByte(index, (Byte)x);
        else if (x instanceof Short)
            setShort(index, (Short)x);
        else if (x instanceof Integer)
            setInt(index, (Integer)x);
        else if (x instanceof Long)
            setLong(index, (Long)x);
        else if (x instanceof Float)
            setFloat(index, (Float)x);
        else if (x instanceof Double)
            setDouble(index, (Double)x);
        else if (x instanceof byte[])
            setBytes(index, (byte[])x);
        else if (x instanceof Date)
            setDate(index, (Date)x);
        else if (x instanceof Time)
            setTime(index, (Time)x);
        else if (x instanceof Timestamp)
            setTimestamp(index, (Timestamp)x);
        else if (x instanceof Array)
            setArray(index, (Array)x);
        else if (x instanceof Blob)
            setBlob(index, (Blob)x);
        else if (x instanceof Clob)
            setClob(index, (Clob)x);
        else if (x instanceof Ref)
            setRef(index, (Ref)x);
        else if (x instanceof URL)
            setURL(index, (URL)x);
        else if (x instanceof RowId)
            setRowId(index, (RowId)x);
        else if (x instanceof NClob)
            setNClob(index, (NClob)x);
        else if (x instanceof SQLXML)
            setSQLXML(index, (SQLXML)x);
        else if (x instanceof BigInteger)
            setLong(index, ((BigInteger)x).longValue());
        else 
            throw new UnsupportedOperationException("Unsupported type " + x);
    }

    public void setNString(int index, String value) {
        setString(index, value);
    }

    public void setAsciiStream(int index, InputStream x, int length) throws IOException {
        String value;
        byte[] b = new byte[length];
        int l = x.read(b);
        value = new String(b, 0, l, "ASCII");
        setValue(index, value, jdbcInstance(Types.VARCHAR));
    }

    public void setUnicodeStream(int index, InputStream x, int length) throws IOException {
        String value;
        byte[] b = new byte[length];
        int l = x.read(b);
        value = new String(b, 0, l, "UTF-8");
        setValue(index, value, jdbcInstance(Types.VARCHAR));
    }

    public void setBinaryStream(int index, InputStream x, int length) throws IOException {
        WrappingByteSource value;
        byte[] b = new byte[length];
        int l = x.read(b);
        value = new WrappingByteSource().wrap(b, 0, l);
        setValue(index, value, jdbcInstance(Types.VARBINARY));
    }

    public void setCharacterStream(int index, Reader reader, int length) throws IOException {
        String value;
        char[] c = new char[length];
        int l = reader.read(c);
        value = new String(c, 0, l);
        setValue(index, value, jdbcInstance(Types.VARCHAR));
    }

    public void setNCharacterStream(int index, Reader value, long length) throws IOException {
        setCharacterStream(index, value, length);
    }

    public void setAsciiStream(int index, InputStream x, long length) throws IOException {
        setAsciiStream(index, x, (int)length);
    }

    public void setBinaryStream(int index, InputStream x, long length) throws IOException {
        setBinaryStream(index, x, (int)length);
    }

    public void setCharacterStream(int index, Reader reader, long length) throws IOException {
        setCharacterStream(index, reader, (int)length);
    }

    public void setAsciiStream(int index, InputStream x) throws IOException {
        String value;
        ByteArrayOutputStream ostr = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        while (true) {
            int len = x.read(buf);
            if (len < 0) break;
            ostr.write(buf, 0, len);
        }
        value = new String(ostr.toByteArray(), "ASCII");
        setValue(index, value, jdbcInstance(Types.VARCHAR));
    }

    public void setBinaryStream(int index, InputStream x) throws IOException {
        WrappingByteSource value;
        ByteArrayOutputStream ostr = new ByteArrayOutputStream();
        byte[] buf = new byte[1024];
        while (true) {
            int len = x.read(buf);
            if (len < 0) break;
            ostr.write(buf, 0, len);
        }
        value = new WrappingByteSource(ostr.toByteArray());
        setValue(index, value, jdbcInstance(Types.VARBINARY));
    }

    public void setCharacterStream(int index, Reader reader) throws IOException {
        String value;
        StringWriter ostr = new StringWriter();
        char[] buf = new char[1024];
        while (true) {
            int len = reader.read(buf);
            if (len < 0) break;
            ostr.write(buf, 0, len);
        }
        value = ostr.toString();
        setValue(index, value, jdbcInstance(Types.VARCHAR));
    }

    public void setNCharacterStream(int index, Reader value) throws IOException {
        setCharacterStream(index, value);
    }

    public void setRef(int index, Ref x) {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int index, Blob x) {
        throw new UnsupportedOperationException();
    }

    public void setClob(int index, Clob x) {
        throw new UnsupportedOperationException();
    }

    public void setClob(int index, Reader reader) {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int index, InputStream inputStream) {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int index, Reader reader) {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int index, NClob value) {
        throw new UnsupportedOperationException();
    }

    public void setClob(int index, Reader reader, long length) {
        throw new UnsupportedOperationException();
    }

    public void setBlob(int index, InputStream inputStream, long length) {
        throw new UnsupportedOperationException();
    }

    public void setNClob(int index, Reader reader, long length) {
        throw new UnsupportedOperationException();
    }

    public void setSQLXML(int index, SQLXML xmlObject) {
        throw new UnsupportedOperationException();
    }

    public void setURL(int index, URL x) {
        throw new UnsupportedOperationException();
    }

    public void setRowId(int index, RowId x) {
        throw new UnsupportedOperationException();
    }

    public void setArray(int index, Array x) {
        throw new UnsupportedOperationException();
    }

    public void formatAsJson(int index, AkibanAppender appender, FormatOptions options) {
        ValueSource value = getValue(index);
        value.getType().formatAsJson(value, appender, options);
    }
}
