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

abstract class ConverterForLong extends LongConverter {

    static final LongConverter LONG = new ConverterForLong() {
        @Override
        protected void putLong(ConversionTarget target, long value) {
            target.putLong(value);
        }

        // AbstractConverter interface

        @Override
        protected AkType nativeConversionType() {
            return AkType.LONG;
        }
    };

    static final LongConverter INT = new ConverterForLong() {
        @Override
        protected void putLong(ConversionTarget target, long value) {
            target.putInt(value);
        }

        // AbstractConverter interface

        @Override
        protected AkType nativeConversionType() {
            return AkType.INT;
        }
    };

    static final LongConverter U_INT = new ConverterForLong() {
        @Override
        protected void putLong(ConversionTarget target, long value) {
            target.putUInt(value);
        }

        // AbstractConverter interface

        @Override
        protected AkType nativeConversionType() {
            return AkType.U_INT;
        }
    };

    @Override
    public String asString(long value) {
        return Long.toString(value);
    }

    @Override
    public long doParse(String string) {
        return Long.parseLong(string);
    }

    @Override
    public long getLong(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case LONG:      return source.getLong();
        case INT:       return source.getInt();
        case U_INT:     return source.getUInt();
        case TEXT:      return Long.parseLong(source.getText());
        case VARCHAR:   return Long.parseLong(source.getString());
        default: throw unsupportedConversion(source);
        }
    }

    private ConverterForLong() {}
}
