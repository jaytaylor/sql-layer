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

package com.akiban.server;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceHelper;
import com.akiban.server.types.ValueSourceIsNullException;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import com.persistit.Value;

import java.math.BigDecimal;
import java.math.BigInteger;

/*
 * This class is used to retrieve a sequence of values from a PersistitValue in stream mode.
 * setExpectedType is used to set the expected type of the next value in the stream.
 */

public final class PersistitValueValueSource extends ValueSource
{

    // PersistitKeyValueSource interface

    public void attach(Value value)
    {
        this.value = value;
        clear();
        value.setStreamMode(true);
    }

    // ValueSource interface

    @Override
    public boolean isNull()
    {
        return decode() == null;
    }

    @Override
    public BigDecimal getDecimal()
    {
        return as(BigDecimal.class, AkType.DECIMAL);
    }

    @Override
    public BigInteger getUBigInt()
    {
        return as(BigInteger.class, AkType.U_BIGINT);
    }

    @Override
    public ByteSource getVarBinary()
    {
        byte[] bytes = as(byte[].class, AkType.VARBINARY);
        return bytes == null ? null : byteSource.wrap(bytes);
    }

    @Override
    public double getDouble()
    {
        return as(Double.class, AkType.DOUBLE);
    }

    @Override
    public double getUDouble()
    {
        return as(Double.class, AkType.U_DOUBLE);
    }

    @Override
    public float getFloat()
    {
        return as(Float.class, AkType.FLOAT);
    }

    @Override
    public float getUFloat()
    {
        return as(Float.class, AkType.U_FLOAT);
    }

    @Override
    public long getDate()
    {
        return as(Long.class, AkType.DATE);
    }

    @Override
    public long getDateTime()
    {
        return as(Long.class, AkType.DATETIME);
    }

    @Override
    public long getInt()
    {
        return as(Long.class, AkType.INT);
    }

    @Override
    public long getLong()
    {
        return as(Long.class, AkType.LONG);
    }

    @Override
    public long getTime()
    {
        return as(Long.class, AkType.TIME);
    }

    @Override
    public long getTimestamp()
    {
        return as(Long.class, AkType.TIMESTAMP);
    }
    
    @Override
    public long getInterval_Millis()
    {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public long getInterval_Month()
    {
        throw new UnsupportedOperationException("interval not supported yet");
    }

    @Override
    public long getUInt()
    {
        return as(Long.class, AkType.U_INT);
    }

    @Override
    public long getYear()
    {
        return as(Long.class, AkType.YEAR);
    }

    @Override
    public String getString()
    {
        return as(String.class, AkType.VARCHAR);
    }

    @Override
    public String getText()
    {
        return as(String.class, AkType.TEXT);
    }

    @Override
    public boolean getBool()
    {
        return as(Boolean.class, AkType.BOOL);
    }

    @Override
    public Cursor getResultSet()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote)
    {
        // Can we optimize this at all?
        AkType type = getConversionType();
        quote.quote(appender, type);
        quote.append(appender, String.valueOf(decode()));
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType()
    {
        return akType;
    }

    // PersistitValueValueSource interface

    public void expectedType(AkType akType)
    {
        this.akType = akType;
        this.needsDecoding = true;
    }

    // object interface

    @Override
    public String toString()
    {
        return value.toString();
    }

    // for use by this class

    private Object decode()
    {
        if (needsDecoding) {
            decoded = value.get();
            needsDecoding = false;
        }
        return decoded;
    }

    private void clear()
    {
        needsDecoding = true;
    }

    private <T> T as(Class<T> castClass, AkType type)
    {
        ValueSourceHelper.checkType(akType, type);
        Object o = decode();
        if (o == null) {
            throw new ValueSourceIsNullException();
        }
        try {
            return castClass.cast(o);
        } catch (ClassCastException e) {
            throw new ClassCastException("casting " + o.getClass() + " to " + castClass);
        }
    }

    // object state

    private Value value;
    private AkType akType = AkType.UNSUPPORTED;
    private Object decoded;
    private boolean needsDecoding = true;
    private final WrappingByteSource byteSource = new WrappingByteSource();
}
