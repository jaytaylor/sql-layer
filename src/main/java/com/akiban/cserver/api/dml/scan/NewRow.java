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
     * @param columnId the column to get
     * @return the value at the specified index, or null if there is none
     * @throws UnsupportedOperationException if not supported
     */
    Object get(ColumnId columnId);

    /**
     * Whether a value is defined in this column. This is the equivalent of Map.containsKey.
     * @param columnId the column to request
     * @return whether a value is defined for the given column
     */
    boolean hasValue(ColumnId columnId);

    /**
     * Removes a value from the row, if it existed. Returns back the old value
     * @param columnId the column whose value we should remove
     * @return the old value, or null if there wasn't one
     */
    Object remove(ColumnId columnId);

    /**
     * Returns an modifiable map view of the fields. The modifying the NewRow will update the Map, and updating
     * the Map will modify the NewRow. The Map must support all optional operations.
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
