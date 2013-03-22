
package com.akiban.server.types;

import com.akiban.qp.operator.Cursor;
import com.akiban.server.Quote;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public final class NullValueSource extends ValueSource {

    public static ValueSource only() {
        return INSTANCE;
    }
    
    // ValueSource interface

    @Override
    public boolean isNull() {
        return true;
    }

    @Override
    public BigDecimal getDecimal() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public BigInteger getUBigInt() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public ByteSource getVarBinary() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public double getDouble() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public double getUDouble() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public float getFloat() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public float getUFloat() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getDate() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getDateTime() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getInt() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getLong() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getTime() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getTimestamp() {
        throw new ValueSourceIsNullException();
    }
    
    @Override
    public long getInterval_Millis() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getInterval_Month() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getUInt() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public long getYear() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public String getString() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public String getText() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public boolean getBool() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public Cursor getResultSet() {
        throw new ValueSourceIsNullException();
    }

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        AkType type = getConversionType();
        quote.quote(appender, type);
        appender.append("null");
        quote.quote(appender, type);
    }

    @Override
    public AkType getConversionType() {
        return AkType.NULL;
    }

    @Override
    public String toString (){
        return "NULL";
    }

    // hidden ctor

    private NullValueSource() {}
    
    // class state

    private static final NullValueSource INSTANCE = new NullValueSource();
}
