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

final class ConverterForDouble extends DoubleConverter {

    static final DoubleConverter INSTANCE = new ConverterForDouble();

    @Override
    public double getDouble(ConversionSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case DOUBLE:    return source.getDouble();
        case FLOAT:     return source.getFloat();
        case DECIMAL:   return source.getDecimal().doubleValue();
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case U_FLOAT:   return source.getFloat();
        case U_DOUBLE:  return source.getUDouble();
        case TEXT:      return Double.parseDouble(source.getText());
        case VARCHAR:   return Double.parseDouble(source.getString());
        default: throw unsupportedConversion(source);
        }
    }

    @Override
    protected void putDouble(ConversionTarget target, double value) {
        target.putDouble(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType nativeConversionType() {
        return AkType.DOUBLE;
    }

    private ConverterForDouble() {}
}
