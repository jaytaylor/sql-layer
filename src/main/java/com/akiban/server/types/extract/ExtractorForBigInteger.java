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
        case DECIMAL:   return BigInteger.valueOf(source.getDecimal().longValueExact());
        case U_DOUBLE:  return BigInteger.valueOf((long)source.getUDouble());
        case DOUBLE:    return BigInteger.valueOf((long)source.getDouble());
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
