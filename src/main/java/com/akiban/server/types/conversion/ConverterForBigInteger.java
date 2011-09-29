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

package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.Extractors;

import java.math.BigInteger;

final class ConverterForBigInteger extends ObjectConverter<BigInteger> {

    static final ObjectConverter<BigInteger> INSTANCE = new ConverterForBigInteger();

    @Override
    protected void putObject(ValueTarget target, BigInteger value) {
        target.putUBigInt(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.U_BIGINT;
    }

    private ConverterForBigInteger() {
        super(Extractors.getUBigIntExtractor());
    }
}
