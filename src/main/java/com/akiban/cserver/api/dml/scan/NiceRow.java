package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.*;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.IdResolver;
import com.akiban.cserver.encoding.Encoding;
import com.akiban.util.ArgumentValidation;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class NiceRow {
    private final Map<ColumnId,Object> fields = new HashMap<ColumnId, Object>();

    public Object put(ColumnId index, Object object) {
        ArgumentValidation.notNull("column", index);
        return fields.put(index, object);
    }

    public Object get(int index) {
        return fields.get(index);
    }

    /**
     * Returns an unmodifiable view of the fields
     * @return the fields that have been set
     */
    public Map<ColumnId,Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }

    public RowData toRowData(RowDef rowDef, IdResolver resolver) {
        final int fieldsOffset = 0
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
        for (Map.Entry<ColumnId,Object> entry : fields.entrySet()) {
            final ColumnId index = entry.getKey();
            final Object obj = entry.getValue();
            final FieldDef fieldDef = rowDef.getFieldDef(index.getPosition(resolver));
            final Encoding<?> encoding = fieldDef.getEncoding();
            bytesLength += encoding.widthFromObject(fieldDef, obj);
        }

        final Object[] objects = new Object[ rowDef.getFieldCount() ];
        for (Map.Entry<ColumnId,Object> entry : fields.entrySet()) {
            objects[ entry.getKey().getPosition(resolver) ] = entry.getValue();
        }
        final RowData retval = new RowData(new byte[bytesLength]);
        retval.createRow(rowDef, objects);

        return retval;
    }

    private void writeNullMap(ByteBuffer buffer, Set<Integer> integers) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        NiceRow niceRow = (NiceRow) o;

        return fields.equals(niceRow.fields);
    }

    @Override
    public int hashCode() {
        return fields.hashCode();
    }
}
