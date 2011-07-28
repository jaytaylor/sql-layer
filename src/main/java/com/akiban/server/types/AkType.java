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

public enum AkType {
    LONG {
        @Override
        void notNullConversion(ConversionSource source, ConversionTarget target) {
            target.setLong(source.getLong());
        }
    },

    DATE {
        @Override
        void notNullConversion(ConversionSource source, ConversionTarget target) {
            target.setDate(source.getDate());
        }
    },

    STRING {
        @Override
        void notNullConversion(ConversionSource source, ConversionTarget target) {
            target.setString(source.getString());
        }
    },

    NULL {
        @Override
        void notNullConversion(ConversionSource source, ConversionTarget target) {
            throw new AssertionError("invoking notNullDispatch on NULL");
        }
    },

    UNSUPPORTED {
        @Override
        void notNullConversion(ConversionSource source, ConversionTarget target) {
            throw new UnsupportedOperationException();
        }
    }
    ;

    public void convert(ConversionSource source, ConversionTarget target) {
        if (source.isNull()) {
            target.setNull();
        }
        else {
            notNullConversion(source, target);
        }
    }

    abstract void notNullConversion(ConversionSource source, ConversionTarget target);
}
