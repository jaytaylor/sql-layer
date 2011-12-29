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
