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

package com.akiban.server.types.extract;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;

import java.math.BigDecimal;
import java.math.BigInteger;

final class ExtractorForBigInteger extends ObjectExtractor<BigInteger> {
    @Override
    public BigInteger getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case U_BIGINT:  return source.getUBigInt();
        case LONG:      return BigInteger.valueOf(source.getLong());
        case INT:       return BigInteger.valueOf(source.getInt());
        case U_INT:     return BigInteger.valueOf(source.getUInt());
        case TEXT:      return new BigInteger(source.getText());
        case VARCHAR:   return new BigInteger(source.getString());
        case INTERVAL_MILLIS:  return BigInteger.valueOf(source.getInterval_Millis());
        case INTERVAL_MONTH:   return BigInteger.valueOf(source.getInterval_Month());
        case DECIMAL:   return source.getDecimal().setScale(0, BigDecimal.ROUND_HALF_UP).toBigInteger();
        case U_DOUBLE:  return BigInteger.valueOf(Math.round(source.getUDouble()));
        case DOUBLE:    return BigInteger.valueOf(Math.round(source.getDouble()));
        case FLOAT:     return BigInteger.valueOf(Math.round(source.getFloat()));
        case U_FLOAT:   return BigInteger.valueOf(Math.round(source.getUFloat()));
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public BigInteger getObject(String string) {
        return new BigInteger(string);
    }

    ExtractorForBigInteger() {
        super(AkType.U_BIGINT);
    }
}
