/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.extract;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;

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
        case DATE:      return BigInteger.valueOf(source.getDate());
        case DATETIME:  return BigInteger.valueOf(source.getDateTime());
        case TIME:      return BigInteger.valueOf(source.getTime());
        case TIMESTAMP: return BigInteger.valueOf(source.getTimestamp());
        case YEAR:      return BigInteger.valueOf(source.getYear());
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
