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

public final class BooleanExtractor extends AbstractExtractor {

    // BooleanExtractor interface

    public Boolean getBoolean(ValueSource source, Boolean ifNull) {
        if (source.isNull())
            return ifNull;
        AkType type = source.getConversionType();
        switch (type) {
        case BOOL:      return source.getBool();
        case VARCHAR:   return getBoolean(source.getString());
        case TEXT:      return getBoolean(source.getText());
        case LONG:      return l2b(source.getLong());
        case DATETIME:  return l2b(source.getDateTime());
        case DATE:      return l2b(source.getDate());
        case TIME:      return l2b(source.getTime());
        case TIMESTAMP: return l2b(source.getTimestamp());
        case YEAR:      return l2b(source.getYear());
        case INT:       return l2b(source.getInt());
        case U_INT:     return l2b(source.getUInt());
        case DOUBLE:
        case FLOAT:
        case U_FLOAT:
        case U_DOUBLE:  return Extractors.getDoubleExtractor().getDouble(source) != 0.0;
        case DECIMAL:   return !source.getDecimal().equals(BigDecimal.ZERO);
        case U_BIGINT:  return !source.getUBigInt().equals(BigInteger.ZERO);
        case INTERVAL_MILLIS:  return l2b(source.getInterval_Millis());
        case INTERVAL_MONTH:   return l2b(source.getInterval_Month());
        default: throw unsupportedConversion(type);
        }
    }

    public boolean getBoolean(String string) {
        if (string == null) return false;
        // JDBC driver passes boolean parameters as "0" and "1".
        return string.equals("1") || string.equalsIgnoreCase("true");
    }

    public String asString(boolean value) {
        return Boolean.toString(value);
    }

    // package-private ctor

    BooleanExtractor() {
        super(AkType.BOOL);
    }

    private boolean l2b(long value) {
        return value != 0;
    }
}
