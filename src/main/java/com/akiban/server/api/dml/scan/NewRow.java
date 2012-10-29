/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.api.dml.scan;

import java.util.Map;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.api.dml.ColumnSelector;

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
