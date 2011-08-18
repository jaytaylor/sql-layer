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

import java.math.BigInteger;

final class ConverterForBigInteger extends ObjectConverter<BigInteger> {

    static final ObjectConverter<BigInteger> INSTANCE = new ConverterForBigInteger();

    @Override
    public BigInteger getObject(ValueSource source) {
        AkType type = source.getConversionType();
        switch (type) {
        case U_BIGINT:  return source.getUBigInt();
        case TEXT:      return new BigInteger(source.getText());
        case VARCHAR:   return new BigInteger(source.getString());
        default: throw unsupportedConversion(source);
        }
    }

    @Override
    protected void putObject(ValueTarget target, BigInteger value) {
        target.putUBigInt(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType nativeConversionType() {
        return AkType.U_BIGINT;
    }

    private ConverterForBigInteger() {}
}
