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

package com.akiban.server.rowdata;

import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.ToObjectValueTarget;

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
