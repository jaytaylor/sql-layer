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

package com.akiban.cserver.api.dml.scan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.encoding.Encoding;
import com.akiban.util.ArgumentValidation;

public class NiceRow extends NewRow {
    private final Map<Integer,Object> fields;
    private final int tableId;

    public NiceRow(int tableId)
    {
        this(tableId, rowDef(tableId));
    }

    public NiceRow(int tableId, RowDef rowDef)
    {
        super(rowDef);
        ArgumentValidation.notNull("tableId", tableId);
        fields = new TreeMap<Integer, Object>();
        this.tableId = tableId;
    }

    @Override
    public Object put(int index, Object object) {
        ArgumentValidation.notNull("column", index);
        return fields.put(index, object);
    }

    @Override
    public int getTableId() {
        return tableId;
    }

    @Override
    public Object get(int column) {
        return fields.get(column);
    }

    @Override
    public boolean hasValue(int columnId) {
        return fields.containsKey(columnId);
    }

    @Override
    public Object remove(int columnId) {
        return fields.remove(columnId);
    }

    @Override
    public Map<Integer,Object> getFields() {
        return fields;
    }

    public static NewRow fromRowData(RowData origData, RowDef rowDef)
    {
        Set<Integer> activeColumns = new HashSet<Integer>();
        for(int fieldIndex=0, fieldsCount=rowDef.getFieldCount(); fieldIndex < fieldsCount; ++fieldIndex) {
            final long location = rowDef.fieldLocation(origData, fieldIndex);
            // Null != not specified. NewRow, NiceRow, RowData all need the concept of specified vs not-specified
            // fields.
            if (true) { // location != 0) {
                activeColumns.add( fieldIndex );
            }
        }

        NewRow retval = new NiceRow(rowDef.getRowDefId(), rowDef);
        for (int pos : activeColumns) {
            final FieldDef fieldDef = rowDef.getFieldDef(pos);
            final Encoding encoding = fieldDef.getEncoding();
            final Object value =
                origData.isNull(fieldDef.getFieldIndex()) ? null : encoding.toObject(fieldDef, origData);
            Object old = retval.put(pos, value);
            assert old == null : String.format("put(%s, %s) --> %s", pos, value, old);
        }

        return retval;
    }

    @Override
    public RowData toRowData() {
        final int fieldsOffset =
                + 4 // record length
                + 2 // signature byte 'AB'
                + 2 // fields count
                + 4 // rowDefId
                + 4 // NullMap
                ;
        int bytesLength =
                fieldsOffset
                // FIELDS GO HERE
                + 2 // signature byte 'BA'
                + 4 // record length again
                ;
        for (int i=0, fieldCount=rowDef.getFieldCount(); i < fieldCount; ++i) {
            bytesLength += rowDef.getFieldDef(i).getMaxStorageSize();
        }

        final Object[] objects = new Object[ rowDef.getFieldCount() ];
        for (Map.Entry<Integer,Object> entry : fields.entrySet()) {
            objects[ entry.getKey() ] = entry.getValue();
        }
        final RowData retval = new RowData(new byte[bytesLength]);
        retval.createRow(rowDef, objects);

        return retval;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NiceRow{ ");
        int nextExpectedPos = 0;
        for (Map.Entry<Integer,Object> entry : fields.entrySet()) {
            final int pos = entry.getKey();
            if (pos != nextExpectedPos) {
                sb.append("... ");
            }
            
            final Object value = entry.getValue();
            sb.append('(').append(entry.getKey()).append(": ");
            if (value != null) {
                sb.append(value.getClass().getSimpleName()).append(' ');
            }
            sb.append(value).append(") ");

            nextExpectedPos = pos + 1;
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiceRow niceRow = (NiceRow) o;

        return fields.equals(niceRow.fields) && (tableId == niceRow.tableId);

    }

    @Override
    public int hashCode() {
        int result = fields != null ? fields.hashCode() : 0;
        result = 31 * result + tableId;
        return result;
    }
}
