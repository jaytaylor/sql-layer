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

package com.akiban.server.store;

import com.akiban.ais.model.Column;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.rowdata.FieldDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDataValueSource;
import com.akiban.server.types.AkType;
import com.akiban.server.types.Converters;
import com.akiban.server.types.FromObjectValueSource;
import com.persistit.Key;

public final class PersistitKeyAppender {

    public void append(int value) {
        key.append(value);
    }

    public void append(long value) {
        key.append(value);
    }

    public void append(Object object, Column column) {
        fromObjectSource.setReflectively(object);
        target.expectingType(column);
        Converters.convert(fromObjectSource, target);
    }

    public void append(Object object, FieldDef fieldDef) {
        append(object, fieldDef.column());
    }

    public void append(FieldDef fieldDef, RowData rowData) {
        fromRowDataSource.bind(fieldDef, rowData);
        target.expectingType(fieldDef.column());
        Converters.convert(fromRowDataSource, target);
    }

    public void appendNull() {
        target.expectingType(AkType.NULL).putNull();
    }

    public Key key() {
        return key;
    }

    public PersistitKeyAppender(Key key) {
        this.key = key;
        fromRowDataSource = new RowDataValueSource();
        fromObjectSource = new FromObjectValueSource();
        target = new PersistitKeyValueTarget();
        target.attach(this.key);
    }

    private final FromObjectValueSource fromObjectSource;
    private final RowDataValueSource fromRowDataSource;
    private final PersistitKeyValueTarget target;
    private final Key key;
}
