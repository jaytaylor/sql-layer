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

package com.akiban.qp.rowtype;

// Fields are untyped for now. Field name is just position within the type.

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

public abstract class RowType
{
    // Object interface

    @Override
    public String toString()
    {
        return String.format("RowType(%s)", typeId);
    }

    @Override
    public int hashCode()
    {
        return typeId * 9987001;
    }

    @Override
    public boolean equals(Object o)
    {
        return o == this || o != null && o instanceof RowType && this.typeId == ((RowType)o).typeId;
    }

    // RowType interface

    public abstract DerivedTypesSchema schema();

    public final int typeId()
    {
        return typeId;
    }

    public final TypeComposition typeComposition()
    {
        return typeComposition;
    }

    public final boolean ancestorOf(RowType that)
    {
        return that.typeComposition != null && this.typeComposition.isAncestorOf(that.typeComposition);
    }

    public final boolean parentOf(RowType that)
    {
        return that.typeComposition != null && this.typeComposition.isParentOf(that.typeComposition);
    }

    public abstract int nFields();

    public abstract AkType typeAt(int index);

    public abstract TInstance typeInstanceAt(int index);

    public HKey hKey()
    {
        return null;
    }

    /**
     * <p>Gets the AIS Column for a given field. This method may throw an exception if the RowType doesn't have
     * an AIS column at the given index, for whatever reason. This may be because the question doesn't make sense
     * ({@code SELECT 1;}), or because the implementation doesn't track information that it technically could.</p>
     *
     * <p>Regardless, you can invoke {@linkplain #fieldHasColumn(int)} to determine if this method will throw
     * an exception. If that method returns {@code true}, this method may not throw an exception.</p>
     * @param field the field index for which you want a column
     * @return the Column that corresponds to that field
     * @throws FieldHasNoColumnException if the field doesn't correspond to any Column
     * @throws IndexOutOfBoundsException if {@code field >= nFields() || field < 0}
     */
    public Column fieldColumn(int field) {
        checkFieldRange(field);
        throw new FieldHasNoColumnException(field);
    }

    /**
     * <p>Returns whether the given field corresponds to a Column. If it does, {@linkplain #fieldColumn(int)} will
     * not throw an exception. If this method returns {@code false}, getting the field's column may (and probably will)
     * result in an exception.</p>
     * @param field the field for which you want a column
     * @return whether that field corresponds to a Column
     * @throws IndexOutOfBoundsException if {@code field >= nFields() || field < 0}
     */
    public boolean fieldHasColumn(int field) {
        checkFieldRange(field);
        return false;
    }

    /**
     * <p>Get the one user table that this row type corresponds to. Not all row types correspond to one user table;
     * a flattened row doesn't, nor does a row type that adds fields. For instance, the final row type for a query
     * {@code SELECT cid FROM customer} may (but does not have to) have a corresponding UserTable; the row type
     * for {@code SELECT 1, cid FROM customer} may not, since the first field doesn't correspond to any column
     * in the {@code customer} table.</p>
     *
     * <p>If this row type doesn't correspond to a user table, it will throw an exception. You can test for that
     * using {@linkplain #hasUserTable()}. If that method returns true, this method may not throw an exception.</p>
     *
     * <p>If this method doesn't throw an exception, several other things must be true:
     * <ul>
     *     <li>{@code fieldHasColumn(n) == true} for {@code 0 <= n < nFields()}</li>
     *     <li>{@code fieldColumn(n).getTable() == userTable()} (for same range of {@code n}</li>
     * </ul></p>
     * @return the user table associated with this row
     * @throws RowTypeToTableMappingException if there is no user table associated with this row
     */
    public UserTable userTable() {
        throw new RowTypeToTableMappingException("default RowType implementation has no UserTable");
    }

    public boolean hasUserTable() {
        return false;
    }

    // For use by subclasses

    protected void typeComposition(TypeComposition typeComposition)
    {
        this.typeComposition = typeComposition;
    }

    protected void checkFieldRange(int field) {
        if (field < 0) {
            throw new IndexOutOfBoundsException("field index must be >= 0: was " + field);
        }
        if (field >= nFields()) {
            throw new IndexOutOfBoundsException(String.format("field (%d) >= fields count (%d)", field, nFields()));
        }
    }

    protected RowType(int typeId)
    {
        this.typeId = typeId;
    }

    // Object state

    private final int typeId;
    private TypeComposition typeComposition;
}
