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
    DATE (ConvertersForDates.DATE),
    DATETIME (ConvertersForDates.DATETIME),
    DECIMAL (ConverterForBigDecimal.INSTANCE),
    DOUBLE (ConverterForDouble.INSTANCE),
    FLOAT (ConverterForFloat.INSTANCE),
    INT (ConverterForLong.INT),
    LONG (ConverterForLong.LONG),
    VARCHAR (ConverterForString.STRING),
    TEXT (ConverterForString.TEXT),
    TIME (ConvertersForDates.TIME),
    TIMESTAMP (ConvertersForDates.TIMESTAMP),
    U_BIGINT (ConverterForBigInteger.INSTANCE),
    U_DOUBLE (ConverterForDouble.INSTANCE),
    U_FLOAT (ConverterForFloat.INSTANCE),
    U_INT (ConverterForLong.U_INT),
    VARBINARY (ConverterForVarBinary.INSTANCE),
    YEAR (ConvertersForDates.YEAR),
    NULL (null),
    UNSUPPORTED (null),
    ;


    AbstractConverter converter() {
        if (converter == null) {
            throw new UnsupportedOperationException("no converter for " + name());
        }
        return converter;
    }

    AkType(AbstractConverter converter) {
        this.converter = converter;
    }

    private final AbstractConverter converter;
}