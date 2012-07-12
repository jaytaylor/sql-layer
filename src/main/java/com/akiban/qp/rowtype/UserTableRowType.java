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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;
import com.akiban.util.FilteringIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UserTableRowType extends AisRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return table.toString();
    }

    // RowType interface

    @Override
    public int nFields()
    {
        return table.getColumnsIncludingInternal().size();
    }

    @Override
    public AkType typeAt(int index)
    {
        return akTypes[index];
    }

    @Override
    public HKey hKey()
    {
        return table.hKey();
    }

    @Override
    public TInstance typeInstanceAt(int index) {
        return table.getColumnsIncludingInternal().get(index).tInstance();
    }

    // UserTableRowType interface
    @Override
    public UserTable userTable()
    {
        return table;
    }

    @Override
    public boolean hasUserTable() {
        return table != null;
    }

    public IndexRowType indexRowType(Index index)
    {
        return indexRowTypes.get(index.getIndexId());
    }

    public void addIndexRowType(IndexRowType indexRowType)
    {
        Index index = indexRowType.index();
        int requiredEntries = index.getIndexId() + 1;
        while (indexRowTypes.size() < requiredEntries) {
            indexRowTypes.add(null);
        }
        indexRowTypes.set(index.getIndexId(), indexRowType);
    }

    public Iterable<IndexRowType> indexRowTypes() {
        return new Iterable<IndexRowType>() {
            @Override
            public Iterator<IndexRowType> iterator() {
                return new FilteringIterator<IndexRowType>(indexRowTypes.iterator(), false) {
                    @Override
                    protected boolean allow(IndexRowType item) {
                        return item != null;
                    }
                };
            }
        };
    }

    public UserTableRowType(Schema schema, UserTable table)
    {
        super(schema, table.getTableId());
        this.table = table;
        typeComposition(new SingleBranchTypeComposition(this, table));
        List<Column> columns = table.getColumnsIncludingInternal();
        akTypes = new AkType[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            akTypes[i] = column.getType().akType();
        }
    }

    // Object state

    private final UserTable table;
    // Type of indexRowTypes is ArrayList, not List, to make it clear that null values are permitted.
    private final ArrayList<IndexRowType> indexRowTypes = new ArrayList<IndexRowType>();
    private final AkType[] akTypes;
}
