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

import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.error.OverflowException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueSourceIsNullException;
import java.math.BigDecimal;
import java.math.BigInteger;

public final class DoubleExtractor extends AbstractExtractor {

    public double getDouble(ValueSource source) {
        if (source.isNull())
            throw new ValueSourceIsNullException();

        AkType type = source.getConversionType();
        switch (type) {
        case DECIMAL:   return getDecimalAsDouble(source);
        case DOUBLE:    return source.getDouble();
        case FLOAT:     return source.getFloat();
        case INT:       return source.getInt();
        case LONG:      return source.getLong();
        case VARCHAR:   return getDouble(source.getString());
        case TEXT:      return getDouble(source.getText());
        case U_BIGINT:  return getBigIntAsDouble(source);
        case U_DOUBLE:  return source.getUDouble();
        case U_FLOAT:   return source.getUFloat();
        case U_INT:     return source.getUInt();
        case INTERVAL_MILLIS:  return source.getInterval_Millis();
        case INTERVAL_MONTH:   return source.getInterval_Month();
        default:
            throw unsupportedConversion(type);
        }                
    }

    private double getDecimalAsDouble (ValueSource source )
    {
        double d = source.getDecimal().doubleValue();
        if (Double.isInfinite(d))
            throw new OverflowException();        
        else
            return d;
    }

    private double getBigIntAsDouble (ValueSource source)
    {
        double d = source.getUBigInt().doubleValue();
        if (Double.isInfinite(d))
            throw new OverflowException();        
        else
            return d;
    }

    public double getDouble(String string) {
        try
        {
            return Double.parseDouble(string);
        }
        catch (NumberFormatException e)
        {
            throw new InvalidCharToNumException(e.getMessage());
        }
    }

    public String asString(double value) {
        return Double.toString(value);
    }

    // package-private ctor
    DoubleExtractor() {
        super(AkType.DOUBLE);
    }
}
