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

final class ExtractorForString extends ObjectExtractor<String> {
    @Override
    public String getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case BOOL:      return Boolean.toString(source.getBool());
        case TEXT:      return source.getText();
        case NULL:      return "null";
        case VARCHAR:   return source.getString();
        case DOUBLE:    return Double.toString(source.getDouble());
        case FLOAT:     return Float.toString(source.getFloat());
        case INT:       return Long.toString(source.getInt());
        case LONG:      return Long.toString(source.getLong());
        case U_INT:     return Long.toString(source.getUInt());
        case U_DOUBLE:  return Double.toString(source.getUDouble());
        case U_FLOAT:   return Float.toString(source.getUFloat());
        case U_BIGINT:  return String.valueOf(source.getUBigInt());
        case TIME:      return longExtractor(AkType.TIME).asString(source.getTime());
        case TIMESTAMP: return longExtractor(AkType.TIMESTAMP).asString(source.getTimestamp());
        case YEAR:      return longExtractor(AkType.YEAR).asString(source.getYear());
        case DATE:      return longExtractor(AkType.DATE).asString(source.getDate());
        case DATETIME:  return longExtractor(AkType.DATETIME).asString(source.getDateTime());
        case DECIMAL:   return String.valueOf(source.getDecimal());
        case VARBINARY: return String.valueOf(source.getVarBinary());
        case INTERVAL:  return longExtractor(AkType.INTERVAL).asString(source.getInterval());
        default:
            throw unsupportedConversion(type);
        }
    }

    @Override
    public String getObject(String string) {
        return string;
    }

    private static LongExtractor longExtractor(AkType forType) {
        // we could also cache these, since they're static
        return Extractors.getLongExtractor(forType);
    }

    ExtractorForString() {
        super(AkType.VARCHAR);
    }
}
