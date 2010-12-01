package com.akiban.cserver.api.dml.scan;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;

import java.util.Map;

public interface NewRow {
    /**
     * Puts a value into the row. Optional operation.
     * @param index the column to insert into
     * @param object the object to insert
     * @return the previous object at the specified index, or null if there was one
     * @throws UnsupportedOperationException if not supported
     */
    Object put(ColumnId index, Object object);

    /**
     * Gets the table ID to which this row belongs
     * @return the table ID
     */
    TableId getTableId();

    /**
     * Gets the value at the specified index, which is a 0-indexed column position offset.
     * @param index the column position
     * @return the value at the specified index, or null if there is none
     * @throws UnsupportedOperationException if not supported
     */
    Object get(int index);

    /**
     * Returns an unmodifiable view of the fields
     * @return the fields that have been set
     * @throws UnsupportedOperationException if not supported
     */
    Map<ColumnId,Object> getFields();

    /**
     * Specifies whether this instance needs a non-null RowDef for its {@linkplain #toRowData(RowDef)}
     * @return whether a RowDef is needed to convert this row to a RowData
     */
    boolean needsRowDef();

    /**
     * Converts this row to a newly allocated RowData
     * @param rowDef the row's RowDef; will be ignored if {@linkplain #needsRowDef()} is true
     * @return the data represented by this row, encoded as a RowData
     * @throws NullPointerException if rowDef is required but null
     */
    RowData toRowData(RowDef rowDef);
}
