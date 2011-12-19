/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.types.extract;

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
        return Double.parseDouble(string);
    }

    public String asString(double value) {
        return Double.toString(value);
    }

    // package-private ctor
    DoubleExtractor() {
        super(AkType.DOUBLE);
    }
}
