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

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;

abstract class AbstractConverter {
    public final void convert(ValueSource source, ValueTarget target) {
        if (source.isNull()) {
            target.putNull();
        }
        else {
            doConvert(source, target);
        }
    }

    protected InvalidOperationException unsupportedConversion(AkType sourceType) {
        throw new InconvertibleTypesException(sourceType, targetConversionType());
    }

    protected abstract void doConvert(ValueSource source, ValueTarget target);
    protected abstract AkType targetConversionType();
}
