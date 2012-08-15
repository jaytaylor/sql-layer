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

import com.akiban.ais.model.*;
import com.akiban.server.types.AkType;
import com.akiban.server.types3.TInstance;

import java.util.*;

public abstract class IndexRowType extends AisRowType
{
    // Object interface

    @Override
    public String toString()
    {
        return index.toString();
    }

    // RowType interface

    @Override
    public TInstance typeInstanceAt(int i) {
        return index.getAllColumns().get(i).getColumn().tInstance();
    }

    @Override
    public int nFields()
    {
        return akTypes.length;
    }

    @Override
    public AkType typeAt(int index)
    {
        return akTypes[index];
    }
    
    @Override
    public HKey hKey()
    {
        return tableType.hKey();
    }

    // IndexRowType interface
    
    public int declaredFields()
    {
        return index().getKeyColumns().size();
    }

    public UserTableRowType tableType()
    {
        return tableType;
    }

    public Index index()
    {
        return index;
    }

    public IndexRowType physicalRowType()
    {
        return this;
    }

    public static IndexRowType createIndexRowType(Schema schema, UserTableRowType tableType, Index index)
    {
        return new Conventional(schema, tableType, index);
    }

    // For use by subclasses

    protected IndexRowType(Schema schema, UserTableRowType tableType, Index index, int nFields)
    {
        super(schema, schema.nextTypeId());
        if (index.isGroupIndex()) {
            GroupIndex groupIndex = (GroupIndex) index;
            assert groupIndex.leafMostTable() == tableType.userTable();
        }
        this.tableType = tableType;
        this.index = index;
        this.akTypes = new AkType[nFields];
    }

    // Object state

    // If index is a GroupIndex, then tableType.userTable() is the leafmost table of the GroupIndex.
    private final UserTableRowType tableType;
    private final Index index;
    protected final AkType[] akTypes;

    // Inner classes

    private static class Conventional extends IndexRowType
    {
        @Override
        public IndexRowType physicalRowType()
        {
            return spatialIndexRowType;
        }

        public Conventional(Schema schema, UserTableRowType tableType, Index index)
        {
            super(schema, tableType, index, index.getAllColumns().size());
            List<IndexColumn> indexColumns = index.getAllColumns();
            for (int i = 0; i < indexColumns.size(); i++) {
                Column column = indexColumns.get(i).getColumn();
                akTypes[i] = column.getType().akType();
            }
            spatialIndexRowType = index.isSpatial() ? new Spatial(schema, tableType, index) : null;
        }

        // For a spatial index, the IndexRowType reflects the declared columns. physicalRowType reflects the
        // stored index, which replaces the declared columns by a z-value column.
        private final IndexRowType spatialIndexRowType;
    }

    private static class Spatial extends IndexRowType
    {
        @Override
        public IndexRowType physicalRowType()
        {
            assert false;
            return null;
        }

        public Spatial(Schema schema, UserTableRowType tableType, Index index)
        {
            super(schema, tableType, index, index.getAllColumns().size() - index.getKeyColumns().size() + 1);
            int t = 0;
            akTypes[t++] = AkType.LONG;
            List<IndexColumn> indexColumns = index.getAllColumns();
            for (int i = index.getKeyColumns().size(); i < indexColumns.size(); i++) {
                Column column = indexColumns.get(i).getColumn();
                akTypes[t++] = column.getType().akType();
            }
        }
    }
}
