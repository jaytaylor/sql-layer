
package com.akiban.server.types;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.server.Quote;
import com.akiban.server.collation.AkCollator;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class ValueSource {

    public abstract boolean isNull();

    public abstract BigDecimal getDecimal();

    public abstract BigInteger getUBigInt();

    public abstract ByteSource getVarBinary();

    public abstract double getDouble();

    public abstract double getUDouble();

    public abstract float getFloat();

    public abstract float getUFloat();

    public abstract long getDate();

    public abstract long getDateTime();

    public abstract long getInt();

    public abstract long getLong();

    public abstract long getTime();

    public abstract long getTimestamp();

    public abstract long getInterval_Millis();

    public abstract long getInterval_Month();

    public abstract long getUInt();

    public abstract long getYear();

    public abstract String getString();

    public abstract String getText();

    public abstract boolean getBool();

    public abstract Cursor getResultSet();

    public abstract void appendAsString(AkibanAppender appender, Quote quote);

    public abstract AkType getConversionType();

    public long hash(StoreAdapter adapter, AkCollator collator)
    {
        return
            collator == null
            ? getString().hashCode()
            : adapter.hash(this, collator);
    }
}
