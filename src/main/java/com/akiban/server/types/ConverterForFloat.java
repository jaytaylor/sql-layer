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

package com.akiban.server.types;

final class ConverterForFloat extends FloatConverter {

    static final FloatConverter INSTANCE = new ConverterForFloat();

    @Override
    public float getFloat(ConversionSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case DOUBLE:    return (float) source.getDouble();
        case FLOAT:     return source.getFloat();
        case DECIMAL:   return source.getDecimal().floatValue();
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case U_FLOAT:   return source.getUFloat();
        case U_DOUBLE:  return (float) source.getUDouble();
        default: throw unsupportedConversion(type);
        }
    }

    @Override
    protected void putFloat(ConversionTarget target, float value) {
        target.putFloat(value);
    }

    private ConverterForFloat() {}
}
