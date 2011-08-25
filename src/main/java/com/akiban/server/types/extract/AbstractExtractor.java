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

package com.akiban.server.types.extract;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types.AkType;

public abstract class AbstractExtractor {

    public final AkType targetConversionType() {
        return targetConversionType;
    }
    // for use by subclasses

    protected InvalidOperationException unsupportedConversion(AkType sourceType) {
        throw new InconvertibleTypesException(sourceType, targetConversionType());
    }

    // for use in this package

    AbstractExtractor(AkType targetConversionType) {
        this.targetConversionType = targetConversionType;
    }

    private final AkType targetConversionType;
}
