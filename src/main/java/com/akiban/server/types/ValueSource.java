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
