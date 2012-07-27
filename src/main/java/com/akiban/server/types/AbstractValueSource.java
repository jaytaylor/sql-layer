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
import com.akiban.server.Quote;
import com.akiban.server.types.extract.Extractors;
import com.akiban.util.AkibanAppender;
import com.akiban.util.ByteSource;

import java.math.BigDecimal;
import java.math.BigInteger;

public abstract class AbstractValueSource extends ValueSource {

    // ValueSource interface

    @Override
    public void appendAsString(AkibanAppender appender, Quote quote) {
        appender.append(Extractors.getStringExtractor().getObject(this));
    }
    
    @Override
    public BigDecimal getDecimal() {
        throw complain(AkType.DECIMAL);
    }

    @Override
    public BigInteger getUBigInt() {
        throw complain(AkType.U_BIGINT);
    }

    @Override
    public ByteSource getVarBinary() {
        throw complain(AkType.VARBINARY);
    }

    @Override
    public double getDouble() {
        throw complain(AkType.DOUBLE);
    }

    @Override
    public double getUDouble() {
        throw complain(AkType.U_DOUBLE);
    }

    @Override
    public float getFloat() {
        throw complain(AkType.FLOAT);
    }

    @Override
    public float getUFloat() {
        throw complain(AkType.U_FLOAT);
    }

    @Override
    public long getDate() {
        throw complain(AkType.DATE);
    }

    @Override
    public long getDateTime() {
        throw complain(AkType.DATETIME);
    }

    @Override
    public long getInt() {
        throw complain(AkType.INT);
    }

    @Override
    public long getLong() {
        throw complain(AkType.LONG);
    }

    @Override
    public long getTime() {
        throw complain(AkType.TIME);
    }

    @Override
    public long getTimestamp() {
        throw complain(AkType.TIMESTAMP);
    }

    @Override
    public long getInterval_Millis()
    {
        throw complain(AkType.INTERVAL_MILLIS);
    }

    @Override
    public long getInterval_Month()
    {
        throw complain(AkType.INTERVAL_MONTH);
    }
    
    @Override
    public long getUInt() {
        throw complain(AkType.U_INT);
    }

    @Override
    public long getYear() {
        throw complain(AkType.YEAR);
    }

    @Override
    public String getString() {
        throw complain(AkType.VARCHAR);
    }

    @Override
    public String getText() {
        throw complain(AkType.TEXT);
    }

    @Override
    public boolean getBool() {
        throw complain(AkType.BOOL);
    }
    
    @Override
    public Cursor getResultSet() {
        throw complain(AkType.RESULT_SET);
    }
    
    // for use in this class
    RuntimeException complain(AkType requiredType) {
        AkType actualType = getConversionType();
        return (actualType == requiredType)
                ? new UnsupportedOperationException()
                : new WrongValueGetException(requiredType, actualType);
    }
}
