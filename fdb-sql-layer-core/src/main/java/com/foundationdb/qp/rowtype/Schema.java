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

package com.foundationdb.qp.rowtype;

import com.foundationdb.ais.model.*;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.TPreparedExpression;

import java.util.*;

/** Table RowTypes are indexed by the Table's ID. Derived RowTypes get higher values. */
public class Schema
{
    public static Set<RowType> descendentTypes(RowType ancestorType, Set<? extends RowType> allTypes)
    {
        Set<RowType> descendentTypes = new HashSet<>();
        for (RowType type : allTypes) {
            if (type != ancestorType && ancestorType.ancestorOf(type)) {
                descendentTypes.add(type);
            }
        }
        return descendentTypes;
    }

    public AggregatedRowType newAggregateType(RowType parent, int inputsIndex, List<? extends TInstance> pAggrTypes)
    {
        return new AggregatedRowType(this, nextTypeId(), parent, inputsIndex, pAggrTypes);
    }

    public FlattenedRowType newFlattenType(RowType parent, RowType child)
    {
        return new FlattenedRowType(this, nextTypeId(), parent, child);
    }

    public ProjectedRowType newProjectType(List<? extends TPreparedExpression> tExprs)
    {
        return new ProjectedRowType(this, nextTypeId(), tExprs);
    }

    public ProductRowType newProductType(RowType leftType, TableRowType branchType, RowType rightType)
    {
        return new ProductRowType(this, nextTypeId(), leftType, branchType, rightType);
    }

    public ValuesRowType newValuesType(TInstance... fields)
    {
        return new ValuesRowType(this, nextTypeId(), fields);
    }

    public HKeyRowType newHKeyRowType(HKey hKey)
    {
        return hKeyRowTypes.get(hKey.table().getTableId());
    }

    public BufferRowType bufferRowType(RowType rightType)
    {
        ValuesRowType leftType = newValuesType(InternalIndexTypes.LONG.instance(false));
        return new BufferRowType(this, nextTypeId(), leftType, rightType);
    }

    public TableRowType tableRowType(Table table)
    {
        return tableRowType(table.getTableId());
    }

    public TableRowType tableRowType(int tableID)
    {
        return (TableRowType) rowTypes.get(tableID);
    }

    public IndexRowType indexRowType(Index index)
    {
        return
            index.isTableIndex()
            ? tableRowType(index.leafMostTable()).indexRowType(index)
            : groupIndexRowType((GroupIndex) index);
    }

    public Set<TableRowType> userTableTypes()
    {
        Set<TableRowType> userTableTypes = new HashSet<>();
        for (AisRowType rowType : rowTypes.values()) {
            if (rowType instanceof TableRowType) {
                if (!rowType.table().isAISTable()) {
                    userTableTypes.add((TableRowType) rowType);
                }
            }
        }
        return userTableTypes;
    }
    public Set<RowType> allTableTypes()
    {
        Set<RowType> allTableTypes = new HashSet<>();
        for (RowType rowType : rowTypes.values()) {
            if (rowType instanceof TableRowType) {
                allTableTypes.add(rowType);
            }
        }
        return allTableTypes;
    }

    public List<IndexRowType> groupIndexRowTypes() {
        return groupIndexRowTypes;
    }

    public Schema(AkibanInformationSchema ais)
    {
        this.ais = ais;
        // Create RowTypes for AIS Tables
        for (Table table : ais.getTables().values()) {
            TableRowType tableRowType = new TableRowType(this, table);
            int tableTypeId = tableRowType.typeId();
            rowTypes.put(tableTypeId, tableRowType);
            typeIdToLeast(tableRowType.typeId());
            
            HKeyRowType hKeyRowType = new HKeyRowType (this, nextTypeId(), table.hKey());
            hKeyRowTypes.put(tableTypeId, hKeyRowType);
        }
        // Create RowTypes for AIS TableIndexes
        for (Table table : ais.getTables().values()) {
            TableRowType tableRowType = tableRowType(table);
            for (TableIndex index : table.getIndexesIncludingInternal()) {
                IndexRowType indexRowType = IndexRowType.createIndexRowType(this, nextTypeId(), tableRowType, index);
                tableRowType.addIndexRowType(indexRowType);
                rowTypes.put(indexRowType.typeId(), indexRowType);
            }
        }
        // Create RowTypes for AIS GroupIndexes
        for (Group group : ais.getGroups().values()) {
            for (GroupIndex groupIndex : group.getIndexes()) {
                IndexRowType indexRowType =
                    IndexRowType.createIndexRowType(this, nextTypeId(), tableRowType(groupIndex.leafMostTable()), groupIndex);
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


    private synchronized int nextTypeId()
    {
        return ++typeIdCounter;
    }

    private synchronized void typeIdToLeast(int minValue) {
        typeIdCounter = Math.max(typeIdCounter, minValue);
    }

    // Object state

    private int typeIdCounter = -1;
    private final AkibanInformationSchema ais;
    private final Map<Integer, AisRowType> rowTypes = new HashMap<>();
    private final Map<Integer, HKeyRowType> hKeyRowTypes = new HashMap<>();
    private final List<IndexRowType> groupIndexRowTypes = new ArrayList<>();
}
