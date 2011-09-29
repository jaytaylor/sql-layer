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

import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.DoubleExtractor;
import com.akiban.server.types.extract.Extractors;

abstract class FloatConverter extends AbstractConverter {

    // AbstractFloatConverter interface
    
    // defined in subclasses
    
    protected abstract void putFloat(ValueTarget target, float value);
    
    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putFloat(target, (float)extractor.getDouble(source));
    }

    FloatConverter() {}

    private final DoubleExtractor extractor = Extractors.getDoubleExtractor();
}
