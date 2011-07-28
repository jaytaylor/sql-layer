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

public abstract class AbstractDateSource implements ConversionSource {

    protected abstract long computeDate();

    @Override
    final public long getLong() {
        SourceIsNullException.checkNotNull(this);
        return computeDate();
    }

    @Override
    final public double getDouble() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T getObject(Class<T> requiredClass) {
        throw new UnsupportedOperationException();
    }

    @Override
    final public AkType conversionType() {
        return AkType.DATE;
    }
}
