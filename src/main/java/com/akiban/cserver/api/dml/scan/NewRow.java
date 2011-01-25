package com.akiban.cserver.api.dml.scan;

import java.util.Map;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;

/**
 * <p>A map-like interface for defining rows. This interface does not specify any inherent binding to a row definition;
 * that binding (including any errors that arise from it) should happen when the row is used.</p>
 *
 * <p>Rows should be able to accept <tt>null</tt> values, which should be distinct from unset values. So the following
 * are <strong>not</strong> equivalent:
 * <pre>   {1: 'foo', 2: null }
 * {1: 'foo' }</pre>
 * </p>
 *
 * <p>Note: Although this interface primarily defines a ColumnId -&gt; Object mapping, it is not, and should not be,
 * related to the <tt>Map</tt> interface. This is because the <tt>Map</tt> interface specifies a <tt>hashCode</tt>
 * and <tt>equals</tt> implementation which are not compatible with this class; in particular, this class's equality
 * and hash should take its TableId into consideration.</p>
 */
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
     * Returns a modifiable map view of the fields. The modifying the NewRow will update the Map, and updating
     * the Map will modify the NewRow. The Map must support all optional operations.
     * @return the fields that have been set
     * @throws UnsupportedOperationException if not supported
     */
    Map<ColumnId,Object> getFields();

    /**
     * Converts this row to a newly allocated RowData
     * @return the data represented by this row, encoded as a RowData
     * @throws NullPointerException if rowDef is required but null
     */
    RowData toRowData();

    /**
     * <p>Compares the specified object with this NewRow. Returns <tt>true</tt> if the given object is also a
     * <tt>NewRow</tt>, defines the same (ColumnId, Object) mapping and corresponds to the same TableId.</p>
     *
     * <p>Note that TableIds can only be used in equality and hashcode if they're resolved. That restriction
     * propagates to implementations of this class.</p>
     * @param o the object to compare to
     * @return if the given object is equal to this NewRow
     */
    @Override
    boolean equals(Object o);

    /**
     * <p>Returns the hash code for this NewRow. The hash code is defined as sum of the NewRow's tableId hash code and
     * the hash code for the <tt>Map</tt> returned by {@linkplain #getFields}.</p>
     *
     * <p>Note that TableIds can only be used in equality and hashcode if they're resolved. That restriction
     * propagates to implementations of this class.</p>
     * @return the hash code for this NewRow
     */
    @Override
    int hashCode();
}
