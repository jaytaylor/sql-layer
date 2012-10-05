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

import com.akiban.qp.operator.Cursor;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

/** Make something like an array of typed values (for instance, a
 * <code>Row</code> or a <code>QueryContext</code> accessible using
 * standard Java types, includings ones from the <code>java.sql</code>
 * package.
 * 
 * API like <code>ResultSet</code> or <code>PreparedStatement</code>.
 */
public abstract class ServerJavaValues
{
    protected abstract ValueSource getValue(int index);
    protected abstract PValueSource getPValue(int index);
    protected abstract AkType getTargetType(int index);
    protected abstract void setValue(int index, ValueSource source, AkType akType);
    protected abstract void setPValue(int index, PValueSource source);
    protected abstract ResultSet toResultSet(int index, Cursor resultSet);

    private boolean wasNull;
    private FromObjectValueSource objectSource;

    protected ValueSource value(int index) {
        ValueSource value = getValue(index);
        wasNull = value.isNull();
        return value;
    }

    protected PValueSource pvalue(int index) {
        PValueSource value = getPValue(index);
        wasNull = value.isNull();
        return value;
    }

    protected void setValue(int index, Object value, AkType sourceType) {
        AkType targetType = getTargetType(index);
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
        }
        else {
            ValueSource value = value(index);
            if (wasNull)
                return null;
            else
                return Extractors.getDecimalExtractor().getObject(value);
        }
    }

    public BigDecimal getBigDecimal(int index, int scale) {
        return getBigDecimal(index).setScale(scale);
    }

    public byte[] getBytes(int index) {
        if (Types3Switch.ON) {
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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
            throw new UnsupportedOperationException();
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

    public Object getObject(int index) {
        if (Types3Switch.ON) {
            throw new UnsupportedOperationException();
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

    public <T> T getObject(int index, Class<T> type) {
        throw new UnsupportedOperationException();
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

    public Array getArray(int index) {
        throw new UnsupportedOperationException();
    }

    public URL getURL(int index) {
        throw new UnsupportedOperationException();
    }
}
