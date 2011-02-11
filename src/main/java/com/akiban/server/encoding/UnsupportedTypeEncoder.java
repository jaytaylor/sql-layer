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

package com.akiban.server.encoding;

import com.akiban.ais.model.Type;
import com.akiban.server.FieldDef;
import com.akiban.server.RowData;
import com.persistit.Key;

class UnsupportedTypeEncoder<T> extends EncodingBase<T> {
    private final String name;
    UnsupportedTypeEncoder(String name) {
        this.name = name;
    }

    private UnsupportedOperationException complaint() {
        return new UnsupportedOperationException(name + " is an unsupported type");
    }

    @Override
    public boolean validate(Type type) {
        throw complaint();
    }

    @Override
    public T toObject(FieldDef fieldDef, RowData rowData) throws EncodingException {
        throw complaint();
    }

    @Override
    public int fromObject(FieldDef fieldDef, Object value, byte[] dest, int offset) {
        throw complaint();
    }

    @Override
    public int widthFromObject(FieldDef fieldDef, Object value) {
        throw complaint();
    }

    @Override
    public void toKey(FieldDef fieldDef, RowData rowData, Key key) {
        throw complaint();
    }

    @Override
    public void toKey(FieldDef fieldDef, Object value, Key key) {
        throw complaint();
    }
}
