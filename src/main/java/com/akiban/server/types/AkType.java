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
    DATE        { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putDate( src.getDate() ); } },
    DATETIME    { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putDateTime( src.getDateTime() ); } },
    DECIMAL     { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putDecimal( src.getDecimal() ); } },
    DOUBLE      { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putDouble( src.getDouble() ); } },
    FLOAT       { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putFloat( src.getFloat() ); } },
    INT         { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putInt( src.getInt() ); } },
    LONG        { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putLong( src.getLong() ); } },
    STRING      { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putString( src.getString() ); } },
    TEXT        { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putText( src.getText() ); } },
    TIME        { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putTime( src.getTime() ); } },
    TIMESTAMP   { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putTimestamp( src.getTimestamp() ); } },
    U_BIGINT    { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putUBigInt( src.getUBigInt() ); } },
    U_DOUBLE    { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putUDouble( src.getUDouble() ); } },
    U_FLOAT     { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putUFloat( src.getUFloat() ); } },
    U_INT       { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putUInt( src.getUInt() ); } },
    VARBINARY   { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putVarBinary( src.getVarBinary() ); } },
    YEAR        { @Override void doConversion(ConversionSource src, ConversionTarget tgt) { tgt.putYear( src.getYear() ); } },

    NULL {
        @Override void doConversion(ConversionSource source, ConversionTarget target) {
            throw new AssertionError("invoking doConversion on NULL");
        }
    },

    UNSUPPORTED {
        @Override
        void doConversion(ConversionSource source, ConversionTarget target) {
            throw new UnsupportedOperationException();
        }
    }
    ;

    public void convert(ConversionSource source, ConversionTarget target) {
        if (source.isNull()) {
            target.putNull();
        }
        else {
            doConversion(source, target);
        }
    }

    abstract void doConversion(ConversionSource source, ConversionTarget target);
}