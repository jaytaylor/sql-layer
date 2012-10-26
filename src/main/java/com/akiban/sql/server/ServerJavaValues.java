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

package com.akiban.sql.server;

import com.akiban.server.error.NoSuchCastException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.aksql.aktypes.AkResultSet;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MDatetimes;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.WrappingByteSource;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
    protected abstract int size();
    protected abstract ServerQueryContext getContext();
    protected abstract ValueSource getValue(int index);
    protected abstract PValueSource getPValue(int index);
    protected abstract AkType getAkType(int index);
    protected abstract TInstance getTInstance(int index);
    protected abstract void setValue(int index, ValueSource source, AkType akType);
    protected abstract void setPValue(int index, PValueSource source);
    protected abstract ResultSet toResultSet(int index, Object resultSet);

    private boolean wasNull;
    private FromObjectValueSource objectSource;
    private CachedCast[] cachedCasts;

    protected ValueSource value(int index) {
        ValueSource value = getValue(index);
        wasNull = value.isNull();
        return value;
    }

    protected PValueSource pvalue(int index) {
        PValueSource pvalue = getPValue(index);
        wasNull = pvalue.isNull();
        return pvalue;
    }

    protected static class CachedCast {
        TClass targetClass;
        TCast tcast;
        TExecutionContext tcontext;
        PValue target;

        protected CachedCast(TInstance sourceInstance, TClass targetClass, 
                             ServerQueryContext context) {
            this.targetClass = targetClass;
            TInstance targetInstance = targetClass.instance(sourceInstance == null || sourceInstance.nullability());
            tcast = context.getServer().t3RegistryService().getCastsResolver()
                .cast(sourceInstance, targetInstance);
            if (tcast == null)
                throw new NoSuchCastException(sourceInstance, targetInstance);
            tcontext = new TExecutionContext(Collections.singletonList(sourceInstance),
                                             targetInstance,
                                             context);
            target = new PValue(targetClass.underlyingType());
        }

        protected boolean matches(TClass required) {
            return required.equals(targetClass);
        }

        protected PValueSource apply(PValueSource pvalue) {
            tcast.evaluate(tcontext, pvalue, target);
            return target;
        }
    }

    /** Cast as necessary to <code>requiredClass</code>.
     * A cache is maintained for each index with the last class, on
     * the assumption the caller will be applying the same
     * <code>getXxx</code> to the same field each time.
     */
    protected PValueSource cachedCast(int index, PValueSource pvalue, TClass required) {
        TInstance sourceInstance = getTInstance(index);
        if (required.equals(sourceInstance.typeClass()))
            return pvalue;      // Already of the required class.
        if (cachedCasts == null)
            cachedCasts = new CachedCast[size()];
        CachedCast cast = cachedCasts[index];
        if ((cast == null) || !cast.matches(required)) {
            cast = new CachedCast(sourceInstance, required, getContext());
            cachedCasts[index] = cast;
        }
        return cast.apply(pvalue);
    }

    protected void setValue(int index, Object value, AkType sourceType) {
        AkType targetType = getAkType(index);
        if (Types3Switch.ON) {
            if (sourceType == null)
                sourceType = targetType;
            PValueSource source = PValueSources.fromObject(value, sourceType).value();
            setPValue(index, source);
        }
        else {
            if (objectSource == null)
                objectSource = new FromObjectValueSource();
            if (sourceType != null)
                objectSource.setExplicitly(value, sourceType);
            else
                objectSource.setReflectively(value);
            setValue(index, objectSource, targetType);
        }
    }
    
    public boolean wasNull() {
        return wasNull;
    }

    public String getString(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return cachedCast(index, pvalue, MString.VARCHAR).getString();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return Extractors.getStringExtractor().getObject(value);
        }
    }

    public String getNString(int index) {
        throw new UnsupportedOperationException();
    }

    public boolean getBoolean(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return false;
            else
                return cachedCast(index, pvalue, AkBool.INSTANCE).getBoolean();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return false;
            else
                return Extractors.getBooleanExtractor().getBoolean(value, false);
        }
    }

    public byte getByte(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0;
            else
                return cachedCast(index, pvalue, MNumeric.TINYINT).getInt8();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0;
            else
                return (byte)Extractors.getLongExtractor(AkType.INT).getLong(value);
        }
    }

    public short getShort(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0;
            else
                return cachedCast(index, pvalue, MNumeric.SMALLINT).getInt16();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0;
            else
                return (short)Extractors.getLongExtractor(AkType.INT).getLong(value);
        }
    }

    public int getInt(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0;
            else
                return cachedCast(index, pvalue, MNumeric.INT).getInt32();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0;
            else
                return (int)Extractors.getLongExtractor(AkType.INT).getLong(value);
        }
    }

    public long getLong(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0;
            else
                return cachedCast(index, pvalue, MNumeric.BIGINT).getInt64();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0;
            else
                return Extractors.getLongExtractor(AkType.LONG).getLong(value);
        }
    }

    public float getFloat(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0.0f;
            else
                return cachedCast(index, pvalue, MApproximateNumber.FLOAT).getFloat();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0.0f;
            else
                return (float)Extractors.getDoubleExtractor().getDouble(value);
        }
    }

    public double getDouble(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return 0.0;
            else
                return cachedCast(index, pvalue, MApproximateNumber.DOUBLE).getDouble();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return 0.0;
            else
                return Extractors.getDoubleExtractor().getDouble(value);
        }
    }

    public BigDecimal getBigDecimal(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return (BigDecimal)cachedCast(index, pvalue, MNumeric.DECIMAL).getObject();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return Extractors.getDecimalExtractor().getObject(value);
        }
    }

    public byte[] getBytes(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return cachedCast(index, pvalue, MBinary.VARBINARY).getBytes();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return Extractors.getByteSourceExtractor().getObject(value).toByteSubarray();
        }
    }

    public Date getDate(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return new Date(timestampMillis(index, pvalue));
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return new Date(timestampMillis(value));
        }
    }

    public Time getTime(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return new Time(timestampMillis(index, pvalue));
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return new Time(timestampMillis(value));
        }
    }

    public Timestamp getTimestamp(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else
                return new Timestamp(timestampMillis(index, pvalue));
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return new Timestamp(timestampMillis(value));
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

    protected static long timestampMillis(ValueSource value) {
        return Extractors.getLongExtractor(AkType.TIMESTAMP).getLong(value) * 1000;
    }

    protected long timestampMillis(int index, PValueSource pvalue) {
        return cachedCast(index, pvalue, MDatetimes.TIMESTAMP).getInt32() * 1000L;
    }

    public Object getObject(int index) {
        if (Types3Switch.ON) {
            PValueSource pvalue = pvalue(index);
            if (wasNull)
                return null;
            else {
                TClass tclass = getTInstance(index).typeClass();
                if (tclass.equals(AkBool.INSTANCE))
                    return pvalue.getBoolean();
                else if (tclass.equals(MNumeric.TINYINT))
                    return pvalue.getInt8();
                else if (tclass.equals(MNumeric.SMALLINT))
                    return pvalue.getInt16();
                else if (tclass.equals(MNumeric.INT))
                    return pvalue.getInt32();
                else if (tclass.equals(MNumeric.BIGINT))
                    return pvalue.getInt64();
                else if (tclass.equals(MApproximateNumber.FLOAT))
                    return pvalue.getFloat();
                else if (tclass.equals(MApproximateNumber.DOUBLE))
                    return pvalue.getDouble();
                else if (tclass.equals(MString.CHAR) ||
                         tclass.equals(MString.VARCHAR) ||
                         tclass.equals(MString.TINYTEXT) ||
                         tclass.equals(MString.MEDIUMTEXT) ||
                         tclass.equals(MString.TEXT) ||
                         tclass.equals(MString.LONGTEXT))
                    return pvalue.getString();
                else if (tclass.equals(MBinary.VARBINARY))
                    return pvalue.getBytes();
                else if (tclass.equals(MDatetimes.DATE))
                    return new Date(timestampMillis(index, pvalue));
                else if (tclass.equals(MDatetimes.TIME))
                    return new Time(timestampMillis(index, pvalue));
                else if (tclass.equals(MDatetimes.DATETIME) ||
                         tclass.equals(MDatetimes.TIMESTAMP))
                    return new Timestamp(timestampMillis(index, pvalue));
                else if (tclass.equals(AkResultSet.INSTANCE))
                    return toResultSet(index, pvalue.getObject());
                else
                    return pvalue.getObject();
            }
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else {
                switch (value.getConversionType()) {
                case DATE:
                    return new Date(timestampMillis(value));
                case TIME:
                    return new Time(timestampMillis(value));
                case DATETIME:
                case TIMESTAMP:
                    return new Timestamp(timestampMillis(value));
                case RESULT_SET:
                    return toResultSet(index, value.getResultSet());
                default:
                    return new ToObjectValueTarget().convertFromSource(value);
                }
            }
        }
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
        setValue(index, null, AkType.NULL);
    }

    public void setBoolean(int index, boolean x) {
        setValue(index, x, AkType.BOOL);
    }

    public void setByte(int index, byte x) {
        setValue(index, (int)x, AkType.INT); // TODO: Types3 has separate type.
    }

    public void setShort(int index, short x) {
        setValue(index, (int)x, AkType.INT); // TODO: Types3 has separate type.
    }

    public void setInt(int index, int x) {
        setValue(index, x, AkType.INT);
    }

    public void setLong(int index, long x) {
        setValue(index, x, AkType.LONG);
    }

    public void setFloat(int index, float x) {
        setValue(index, x, AkType.FLOAT);
    }

    public void setDouble(int index, double x) {
        setValue(index, x, AkType.DOUBLE);
    }

    public void setBigDecimal(int index, BigDecimal x) {
        setValue(index, x, AkType.DECIMAL);
    }

    public void setString(int index, String x) {
        setValue(index, x, AkType.VARCHAR);
    }

    public void setBytes(int index, byte x[]) {
        setValue(index, new WrappingByteSource(x), AkType.VARBINARY);
    }

    public void setDate(int index, Date x) {
        // TODO: Aren't there system routines to do this someplace?
        DateTime dt = new DateTime(x, DateTimeZone.getDefault());
        long encoded = dt.getYear() * 512 + dt.getMonthOfYear() * 32 + dt.getDayOfMonth();
        setValue(index, encoded, AkType.DATE);
    }

    public void setTime(int index, Time x) {
        DateTime dt = new DateTime(x, DateTimeZone.getDefault());
        long encoded = dt.getHourOfDay() * 10000 + dt.getMinuteOfHour() * 100 + dt.getSecondOfMinute();
        setValue(index, encoded, AkType.TIME);
    }

    public void setTimestamp(int index, Timestamp x) {
        setValue(index, x.getTime() / 1000, AkType.TIMESTAMP);
    }

    public void setDate(int index, Date x, Calendar cal) {
        cal.setTime(x);
        DateTime dt = new DateTime(cal);
        long encoded = dt.getYear() * 512 + dt.getMonthOfYear() * 32 + dt.getDayOfMonth();
        setValue(index, encoded, AkType.DATE);
    }

    public void setTime(int index, Time x, Calendar cal) {
        cal.setTime(x);
        DateTime dt = new DateTime(cal);
        long encoded = dt.getHourOfDay() * 10000 + dt.getMinuteOfHour() * 100 + dt.getSecondOfMinute();
        setValue(index, encoded, AkType.TIME);
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
        setValue(index, value, AkType.VARCHAR);
    }

    public void setUnicodeStream(int index, InputStream x, int length) throws IOException {
        String value;
        byte[] b = new byte[length];
        int l = x.read(b);
        value = new String(b, 0, l, "UTF-8");
        setValue(index, value, AkType.VARCHAR);
    }

    public void setBinaryStream(int index, InputStream x, int length) throws IOException {
        WrappingByteSource value;
        byte[] b = new byte[length];
        int l = x.read(b);
        value = new WrappingByteSource().wrap(b, 0, l);
        setValue(index, value, AkType.VARBINARY);
    }

    public void setCharacterStream(int index, Reader reader, int length) throws IOException {
        String value;
        char[] c = new char[length];
        int l = reader.read(c);
        value = new String(c, 0, l);
        setValue(index, value, AkType.VARCHAR);
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
        setValue(index, value, AkType.VARCHAR);
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
        setValue(index, value, AkType.VARBINARY);
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
        setValue(index, value, AkType.VARCHAR);
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
}
