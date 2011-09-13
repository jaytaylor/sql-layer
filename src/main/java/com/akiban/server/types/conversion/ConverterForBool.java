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

package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

import java.math.BigInteger;

public final class ConverterForBool extends AbstractConverter {
    @Override
    protected void doConvert(ValueSource source, ValueTarget target) {
        target.putBool(getBool(source));
    }

    @Override
    protected AkType targetConversionType() {
        return AkType.BOOL;
    }

    private boolean getBool(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case DOUBLE:    return source.getDouble() != 0.0d;
        case FLOAT:     return source.getFloat() != 0.0f;
        case INT:       return source.getInt() != 0;
        case LONG:      return source.getLong() != 0;
        case VARCHAR:   return Boolean.parseBoolean(source.getString());
        case TEXT:      return Boolean.parseBoolean(source.getText());
        case U_BIGINT:  return ! source.getUBigInt().equals(BigInteger.ZERO);
        case U_DOUBLE:  return source.getUDouble() != 0.0d;
        case U_FLOAT:   return source.getUFloat() != 0.0f;
        case U_INT:     return source.getUInt() != 0;
        case BOOL:      return source.getBool();
        default: throw unsupportedConversion(type);
        }
    }
}
