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

import java.math.BigDecimal;

final class ConverterForBigDecimal extends ObjectConverter<BigDecimal> {

    static final ObjectConverter<BigDecimal> INSTANCE = new ConverterForBigDecimal();

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
        case DOUBLE:    return BigDecimal.valueOf(source.getDouble());
        default: throw unsupportedConversion(source);
        }
    }

    @Override
    protected void putObject(ValueTarget target, BigDecimal value) {
        target.putDecimal(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType nativeConversionType() {
        return AkType.DECIMAL;
    }

    private ConverterForBigDecimal() {}
}
