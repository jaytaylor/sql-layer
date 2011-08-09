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

abstract class ConverterForString extends ObjectConverter<String> {

    static final ObjectConverter<String> STRING = new ConverterForString() {
        @Override
        protected void putObject(ConversionTarget target, String value) {
            target.putString(value);
        }

        // AbstractConverter interface

        @Override
        protected AkType nativeConversionType() {
            return AkType.VARCHAR;
        }
    };

    static final ObjectConverter<String> TEXT = new ConverterForString() {
        @Override
        protected void putObject(ConversionTarget target, String value) {
            target.putText(value);
        }

        // AbstractConverter interface

        @Override
        protected AkType nativeConversionType() {
            return AkType.TEXT;
        }
    };

    @Override
    public String getObject(ConversionSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case TEXT:      return source.getText();
        case NULL:      return "null";
        case VARCHAR:   return source.getString();
        case DOUBLE:    return Double.toString(source.getDouble());
        case FLOAT:     return Float.toString(source.getFloat());
        case INT:       return Long.toString(source.getInt());
        case LONG:      return Long.toString(source.getLong());
        case U_INT:     return Long.toString(source.getLong());
        case U_DOUBLE:  return Double.toString(source.getUDouble());
        case U_FLOAT:   return Float.toString(source.getFloat());
        case U_BIGINT:  return source.getDecimal().toString();
        case TIME:      // fall
        case TIMESTAMP: // fall
        case VARBINARY: // fall
        case YEAR:      // fall
        default: throw unsupportedConversion(source);
        }
    }

    private ConverterForString() {}
}
