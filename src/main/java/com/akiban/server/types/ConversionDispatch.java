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

public final class ConversionDispatch<T> {
    // public interface
    public T get(AkType type) {
        T result = dispatch(type);
        validate(type, result);
        return result;
    }

    public ConversionDispatch(T longConverter, T dateConverter, T stringConverter) {
        this.longConverter = longConverter;
        this.dateConverter = dateConverter;
        this.stringConverter = stringConverter;
    }

    // private

    private T dispatch(AkType type) {
        switch (type) {
        case LONG:
            return longConverter;
        case DATE:
            return dateConverter;
        case STRING:
            return stringConverter;
        case NULL:
            throw new SourceIsNullException();
        default:
            throw new UnsupportedOperationException(type.name());
        }
    }

    private void validate(AkType type, T longConverter) {
        if (longConverter == null) {
            throw new UnsupportedOperationException(type.name());
        }
    }

    // object state

    private final T longConverter;
    private final T dateConverter;
    private final T stringConverter;
}
