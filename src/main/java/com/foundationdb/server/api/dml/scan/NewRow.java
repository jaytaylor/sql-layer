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

import java.util.Map;

import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.api.dml.ColumnSelector;

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
public abstract class NewRow {
    protected RowDef rowDef;

    /**
     * Gets the RowDef for the row.
     * @return the RowDef
     */
    public final RowDef getRowDef()
    {
        return rowDef;
    }

    /**
     * Puts a value into the row. Optional operation.
     * @param index the column to insert into
     * @param object the object to insert
     * @return the previous object at the specified index, or null if there was one
     * @throws UnsupportedOperationException if not supported
     */
    public abstract Object put(int index, Object object);

    /**
     * Gets the table ID to which this row belongs
     * @return the table ID
     */
    public abstract int getTableId();

    /**
     * Gets the value at the specified index, which is a 0-indexed column position offset.
     * @param columnId the column to get
     * @return the value at the specified index, or null if there is none
     * @throws UnsupportedOperationException if not supported
     */
    public abstract Object get(int columnId);

    /**
     * Whether a value is defined in this column. This is the equivalent of Map.containsKey.
     * @param columnId the column to request
     * @return whether a value is defined for the given column
     */
    public abstract boolean hasValue(int columnId);

    /**
     * Removes a value from the row, if it existed. Returns back the old value
     * @param columnId the column whose value we should remove
     * @return the old value, or null if there wasn't one
     */
    public abstract Object remove(int columnId);

    /**
     * Returns a modifiable map view of the fields. The modifying the NewRow will update the Map, and updating
     * the Map will modify the NewRow. The Map must support all optional operations.
     * @return the fields that have been set
     * @throws UnsupportedOperationException if not supported
     */
    public abstract Map<Integer,Object> getFields();

    /**
     * Converts this row to a newly allocated RowData
     * @return the data represented by this row, encoded as a RowData
     * @throws NullPointerException if rowDef is required but null
     */
    public abstract RowData toRowData();

    /**
     * Returns a ColumnSelector where {@linkplain ColumnSelector#includesColumn(int)} returns true for fields
     * that have been set, as opposed to testing for NULL.
     * @return the ColumnSelector
     */
    public abstract ColumnSelector getActiveColumns();

    /**
     * Returns whether or not the column at the given position is null or unset.
     * @param columnId the column whose value to test
     * @return true if null or unset, false otherwise
     */
    public abstract boolean isColumnNull(int columnId);

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
    public abstract boolean equals(Object o);

    /**
     * <p>Returns the hash code for this NewRow. The hash code is defined as sum of the NewRow's tableId hash code and
     * the hash code for the <tt>Map</tt> returned by {@linkplain #getFields}.</p>
     *
     * <p>Note that TableIds can only be used in equality and hashcode if they're resolved. That restriction
     * propagates to implementations of this class.</p>
     * @return the hash code for this NewRow
     */
    @Override
    public abstract int hashCode();

    protected NewRow(RowDef rowDef)
    {
        this.rowDef = rowDef;
    }
}
