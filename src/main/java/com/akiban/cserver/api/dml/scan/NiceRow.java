package com.akiban.cserver.api.dml.scan;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import com.akiban.cserver.encoding.Encoding;
import com.akiban.util.ArgumentValidation;

public class NiceRow implements NewRow {
    private final Map<ColumnId,Object> fields;
    private final TableId tableId;

    public NiceRow(TableId tableId) {
        ArgumentValidation.notNull("tableId", tableId);
        fields = new TreeMap<ColumnId, Object>();
        this.tableId = tableId;
    }

    @Override
    public Object put(ColumnId index, Object object) {
        ArgumentValidation.notNull("column", index);
        return fields.put(index, object);
    }

    @Override
    public TableId getTableId() {
        return tableId;
    }

    @Override
    public Object get(ColumnId column) {
        return fields.get(column);
    }

    @Override
    public boolean hasValue(ColumnId columnId) {
        return fields.containsKey(columnId);
    }

    @Override
    public Object remove(ColumnId columnId) {
        return fields.remove(columnId);
    }

    @Override
    public Map<ColumnId,Object> getFields() {
        return fields;
    }

    public static NewRow fromRowData(RowData origData, RowDef rowDef) {
        Set<ColumnId> activeColumns = new HashSet<ColumnId>();
        for(int fieldIndex=0, fieldsCount=rowDef.getFieldCount(); fieldIndex < fieldsCount; ++fieldIndex) {
            final long location = rowDef.fieldLocation(origData, fieldIndex);
            if (location != 0) {
                activeColumns.add( ColumnId.of(fieldIndex) );
            }
        }

        NewRow retval = new NiceRow( TableId.of(rowDef.getRowDefId()) );
        for (ColumnId column : activeColumns) {
            final int pos = column.getPosition();
            final FieldDef fieldDef = rowDef.getFieldDef(pos);
            final Encoding encoding = fieldDef.getEncoding();
            final Object value = encoding.toObject(fieldDef, origData);

            Object old = retval.put(column, value);
            assert old == null : String.format("put(%s, %s) --> %s", column, value, old);
        }

        return retval;
    }

    @Override
    public boolean needsRowDef() {
        return true;
    }

    @Override
    public RowData toRowData(RowDef rowDef) {
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
        for (Map.Entry<ColumnId,Object> entry : fields.entrySet()) {
            objects[ entry.getKey().getPosition() ] = entry.getValue();
        }
        final RowData retval = new RowData(new byte[bytesLength]);
        retval.createRow(rowDef, objects);

        return retval;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("NiceRow{ ");
        int nextExpectedPos = 0;
        for (Map.Entry<ColumnId,Object> entry : fields.entrySet()) {
            final int pos = entry.getKey().getPosition();
            if (pos != nextExpectedPos) {
                sb.append("... ");
            }
            
            final Object value = entry.getValue();
            sb.append('(').append(entry.getKey().getPosition()).append(": ");
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

        return fields.equals(niceRow.fields) && tableId.equals(niceRow.tableId);

    }

    @Override
    public int hashCode() {
        return tableId.hashCode() + fields.hashCode();
    }
}
