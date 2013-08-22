/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.ValueTarget;
import com.foundationdb.server.types.extract.ObjectExtractor;

abstract class ObjectConverter<T> extends AbstractConverter {

    // defined in subclasses

    protected abstract void putObject(ValueTarget target, T value);

    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ValueTarget target) {
        putObject(target, extractor.getObject(source));
    }

    ObjectConverter(ObjectExtractor<? extends T> extractor) {
        this.extractor = extractor;
    }

    private final ObjectExtractor<? extends T> extractor;
}
