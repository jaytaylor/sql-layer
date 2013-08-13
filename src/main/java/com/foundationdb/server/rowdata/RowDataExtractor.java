/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.foundationdb.server.rowdata;

import com.foundationdb.server.types.conversion.Converters;
import com.foundationdb.server.types.ToObjectValueTarget;

public final class RowDataExtractor {

    public Object get(FieldDef fieldDef) {
        int f = fieldDef.getFieldIndex();
        RowDataValueSource source = sources[f];
        if (source == null) {
            source = new RowDataValueSource();
            sources[f] = source;
        }
        ToObjectValueTarget target = targets[f];
        if (target == null) {
            target = new ToObjectValueTarget();
            targets[f] = target;
        }
        source.bind(fieldDef, rowData);
        target.expectType(fieldDef.getType().akType());
        return Converters.convert(source, target).lastConvertedValue();
    }

    public RowDataExtractor(RowData rowData, RowDef rowDef)
    {
        this.rowData = rowData;
        assert rowData != null;
        assert rowDef != null;
        assert rowData.getRowDefId() == rowDef.getRowDefId();
        sources = new RowDataValueSource[rowDef.getFieldCount()];
        targets = new ToObjectValueTarget[rowDef.getFieldCount()];
    }

    private final RowData rowData;
    private final RowDataValueSource[] sources;
    private final ToObjectValueTarget[] targets;
}
