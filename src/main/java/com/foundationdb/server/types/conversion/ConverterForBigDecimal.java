/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.conversion;

import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.extract.Extractors;

import java.math.BigDecimal;

final class ConverterForBigDecimal extends ObjectConverter<BigDecimal> {

    static final ObjectConverter<BigDecimal> INSTANCE = new ConverterForBigDecimal();

    @Override
    protected void putObject(ValueTarget target, BigDecimal value) {
        target.putDecimal(value);
    }

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.DECIMAL;
    }

    private ConverterForBigDecimal() {
        super(Extractors.getDecimalExtractor());
    }
}
