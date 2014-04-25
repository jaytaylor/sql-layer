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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.Column;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.rowdata.FieldDef;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataValueSource;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueSources;
import com.persistit.Key;

public abstract class PersistitKeyAppender {

    public final void append(int value) {
        key.append(value);
    }

    public final void append(long value) {
        key.append(value);
    }

    public final void append(Object object, FieldDef fieldDef) {
        append(object, fieldDef.column());
    }

    public final void appendNull() {
        key.append(null);
    }

    public final Key key() {
        return key;
    }

    public final void clear()
    {
        key().clear();
    }

    public abstract void append(Object object, Column column);

    public abstract void append(ValueSource source, Column column);

    public abstract void append(FieldDef fieldDef, RowData rowData);

    public static PersistitKeyAppender create(Key key, Object descForError) {
        return new New(key, descForError);
    }

    protected PersistitKeyAppender(Key key) {
        this.key = key;
    }

    protected final Key key;

    // Inner classes

    private static class New extends PersistitKeyAppender
    {
        public New(Key key, Object descForError) {
            super(key);
            fromRowDataSource = new RowDataValueSource();
            target = new PersistitKeyValueTarget(descForError);
            target.attach(this.key);
        }

        public void append(Object object, Column column) {
            column.getType().writeCollating(ValueSources.valuefromObject(object, column.getType()), target);
        }

        public void append(ValueSource source, Column column) {
            column.getType().writeCollating(source, target);
        }

        public void append(FieldDef fieldDef, RowData rowData) {
            fromRowDataSource.bind(fieldDef, rowData);
            Column column = fieldDef.column();
            column.getType().writeCollating(fromRowDataSource, target);
        }

        private final RowDataValueSource fromRowDataSource;
        private final PersistitKeyValueTarget target;
    }
}
