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

import java.util.*;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class Schema extends DerivedTypesSchema
{
    public UserTableRowType userTableRowType(UserTable table)
    {
        return (UserTableRowType) rowTypes.get(table.getTableId());
    }

    public IndexRowType indexRowType(Index index)
    {
        // TODO: Group index schema is always ""; need another way to
        // check for group _table_ index.
        if (false)
            assert ais.getTable(index.getIndexName().getSchemaName(),
                                index.getIndexName().getTableName()).isUserTable() : index;
        return
            index.isTableIndex()
            ? userTableRowType((UserTable) index.leafMostTable()).indexRowType(index)
            : groupIndexRowType((GroupIndex) index);
    }

    public Set<UserTableRowType> userTableTypes()
    {
        Set<UserTableRowType> userTableTypes = new HashSet<UserTableRowType>();
        for (AisRowType rowType : rowTypes.values()) {
            if (rowType instanceof UserTableRowType) {
                if (!rowType.userTable().isAISTable()) {
                    userTableTypes.add((UserTableRowType) rowType);
                }
            }
        }
        return userTableTypes;
    }

    public Set<RowType> allTableTypes()
    {
        Set<RowType> userTableTypes = new HashSet<RowType>();
        for (RowType rowType : rowTypes.values()) {
            if (rowType instanceof UserTableRowType) {
                userTableTypes.add(rowType);
            }
        }
        return userTableTypes;
    }

    public List<IndexRowType> groupIndexRowTypes() {
        return groupIndexRowTypes;
    }

    public Schema(AkibanInformationSchema ais)
    {
        this.ais = ais;
        // Create RowTypes for AIS UserTables
        for (UserTable userTable : ais.getUserTables().values()) {
            UserTableRowType userTableRowType = new UserTableRowType(this, userTable);
            int tableTypeId = userTableRowType.typeId();
            rowTypes.put(tableTypeId, userTableRowType);
            typeIdToLeast(userTableRowType.typeId());
        }
        // Create RowTypes for AIS TableIndexes
        for (UserTable userTable : ais.getUserTables().values()) {
            UserTableRowType userTableRowType = userTableRowType(userTable);
            for (TableIndex index : userTable.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = IndexRowType.createIndexRowType(this, userTableRowType, index);
                userTableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
            }
        }
        // Create RowTypes for AIS GroupIndexes
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                IndexRowType indexRowType =
                    IndexRowType.createIndexRowType(this, userTableRowType(groupIndex.leafMostTable()), groupIndex);
                rowTypes.put(indexRowType.typeId(), indexRowType);
                groupIndexRowTypes.add(indexRowType);
            }
        }
    }

    public AkibanInformationSchema ais()
    {
        return ais;
    }

    // For use by this package

    // For use by this class

    private IndexRowType groupIndexRowType(GroupIndex groupIndex)
    {
        for (IndexRowType groupIndexRowType : groupIndexRowTypes) {
            if (groupIndexRowType.index() == groupIndex) {
                return groupIndexRowType;
            }
        }
        return null;
    }

    // Object state

    private final AkibanInformationSchema ais;
    private final Map<Integer, AisRowType> rowTypes = new HashMap<Integer, AisRowType>();
    private final List<IndexRowType> groupIndexRowTypes = new ArrayList<IndexRowType>();
}
