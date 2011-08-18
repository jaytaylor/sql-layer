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

public abstract class LongConverter extends AbstractConverter {

    // LongConverter interface

    public abstract long getLong(ValueSource source);
    public abstract String asString(long value);
    public abstract long doParse(String string);

    // defined in subclasses

    protected abstract void putLong(ConversionTarget target, long value);

    // for use in this package

    @Override
    protected final void doConvert(ValueSource source, ConversionTarget target) {
        putLong(target, getLong(source));
    }

    LongConverter() {}
}
