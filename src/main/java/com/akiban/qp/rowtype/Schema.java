/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.qp.rowtype;

import com.akiban.ais.model.*;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.expression.Expression;
import com.akiban.server.types.AkType;

import java.util.*;

import static java.lang.Math.max;

// UserTable RowTypes are indexed by the UserTable's RowDef's ordinal. Derived RowTypes get higher values.

public class Schema extends DerivedTypesSchema
{
    public synchronized UserTableRowType userTableRowType(UserTable table)
    {
        return (UserTableRowType) rowTypes.get(table.getTableId());
    }

    public synchronized IndexRowType indexRowType(Index index)
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

    public synchronized Set<UserTableRowType> userTableTypes()
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

    public synchronized Set<RowType> allTableTypes()
    {
        Set<RowType> userTableTypes = new HashSet<RowType>();
        for (RowType rowType : rowTypes.values()) {
            if (rowType instanceof UserTableRowType) {
                userTableTypes.add(rowType);
            }
        }
        return userTableTypes;
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
                IndexRowType indexRowType = new IndexRowType(this, userTableRowType, index);
                userTableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
            }
        }
        // Create RowTypes for AIS GroupIndexes
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                IndexRowType indexRowType =
                    new IndexRowType(this, userTableRowType(groupIndex.leafMostTable()), groupIndex);
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
