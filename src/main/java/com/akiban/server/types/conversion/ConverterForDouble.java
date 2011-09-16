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

abstract class ConverterForDouble extends DoubleConverter {

    static final DoubleConverter SIGNED = new ConverterForDouble() {
        @Override
        protected void putDouble(ValueTarget target, double value) {
            target.putDouble(value);
        }
    };

    static final DoubleConverter UNSIGNED = new ConverterForDouble() {
        @Override
        protected void putDouble(ValueTarget target, double value) {
            target.putUDouble(value);
        }
    };

    // AbstractConverter interface

    @Override
    protected AkType targetConversionType() {
        return AkType.DOUBLE;
    }

    private ConverterForDouble() {}
}
