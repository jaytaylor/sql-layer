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

package com.foundationdb.server.api.dml.scan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDataExtractor;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.dml.ColumnSelector;
import com.foundationdb.server.api.dml.SetColumnSelector;
import com.foundationdb.util.ArgumentValidation;

public class NiceRow extends NewRow {
    private final static int INITIAL_ROW_DATA_SIZE = RowData.CREATE_ROW_INITIAL_SIZE;

    private final Map<Integer,Object> fields;
    private final int tableId;

    public NiceRow(int tableId, RowDef rowDef)
    {
        super(rowDef);
        ArgumentValidation.notNull("tableId", tableId);
        fields = new TreeMap<>();
        this.tableId = tableId;
    }

    public NiceRow(NewRow copyFrom) {
        this(copyFrom.getTableId(), copyFrom.getRowDef());
        fields.putAll( copyFrom.getFields() );
    }

    @Override
    public Object put(int index, Object object) {
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
        Set<Integer> activeColumns = new HashSet<>();
        for(int fieldIndex=0, fieldsCount=rowDef.getFieldCount(); fieldIndex < fieldsCount; ++fieldIndex) {
            final long location = rowDef.fieldLocation(origData, fieldIndex);
            // Null != not specified. NewRow, NiceRow, RowData all need the concept of specified vs not-specified
            // fields.
            if (true) { // location != 0) {
                activeColumns.add( fieldIndex );
            }
        }

        NewRow retval = new NiceRow(rowDef.getRowDefId(), rowDef);
        RowDataExtractor extractor = new RowDataExtractor(origData, rowDef);
        for (int pos : activeColumns) {
            Object value = extractor.get(rowDef.getFieldDef(pos));
            Object old = retval.put(pos, value);
            assert old == null : String.format("put(%s, %s) --> %s", pos, value, old);
        }

        return retval;
    }

    @Override
    public RowData toRowData() {
        final Object[] objects = new Object[ rowDef.getFieldCount() ];
        for (Map.Entry<Integer,Object> entry : fields.entrySet()) {
            objects[ entry.getKey() ] = entry.getValue();
        }
        final RowData retval = new RowData(new byte[INITIAL_ROW_DATA_SIZE]);
        retval.createRow(rowDef, objects, true);
        return retval;
    }

    @Override
    public ColumnSelector getActiveColumns() {
        return new SetColumnSelector(fields.keySet());
    }

    @Override
    public boolean isColumnNull(int columnId) {
        return get(columnId) == null;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder().append("[{");
        sb.append( rowDef.table().getName().getTableName() );
        if (fields.isEmpty()) {
            return sb.append("} <empty>]").toString();
        }
        sb.append("} ");
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
        sb.setCharAt(sb.length() - 1, ']');
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
