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

final class ExtractorForBigDecimal extends ObjectExtractor<BigDecimal> {
    @Override
    public BigDecimal getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case DECIMAL:   return source.getDecimal();
        case TEXT:      return new BigDecimal(source.getText());
        case VARCHAR:   return new BigDecimal(source.getString());
        case LONG:      return BigDecimal.valueOf(source.getLong());
        case INT:       return BigDecimal.valueOf(source.getInt());
        case U_INT:     return BigDecimal.valueOf(source.getUInt());
        case FLOAT:     return BigDecimal.valueOf(source.getFloat());
        case U_FLOAT:   return BigDecimal.valueOf(source.getUFloat());
        case DOUBLE:    return BigDecimal.valueOf(source.getDouble());
        case U_DOUBLE:  return BigDecimal.valueOf(source.getUDouble());
        case INTERVAL_MILLIS:  return new BigDecimal(source.getInterval_Millis());
        case U_BIGINT:  return new BigDecimal(source.getUBigInt());
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    public BigDecimal getObject(String string) {
        return new BigDecimal(string);
    }

    ExtractorForBigDecimal() {
        super(AkType.DECIMAL);
    }
}
