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

package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.IndexColNotInGroupException;
import com.akiban.server.error.IndexTableNotInGroupException;

public class GroupIndex extends Index
{
    static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, String constraint)
    {
        ais.checkMutability();
        GroupIndex index = new GroupIndex(group, indexName, indexId, isUnique, constraint);
        group.addIndex(index);
        return index;
    }

    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, String constraint, JoinType joinType)
    {
        ais.checkMutability();
        GroupIndex index = new GroupIndex(group, indexName, indexId, isUnique, constraint, joinType);
        group.addIndex(index);
        return index;
    }

    public GroupIndex(Group group, String indexName, Integer indexId, Boolean isUnique, String constraint)
    {
        // index checks index name.
        super(new TableName("", group.getName()), indexName, indexId, isUnique, constraint);
        this.group = group;
    }

    public GroupIndex(Group group, String indexName, Integer indexId, Boolean isUnique, String constraint, JoinType joinType)
    {
        // index checks index name.
        super(new TableName("", group.getName()), indexName, indexId, isUnique, constraint, joinType, true);
        this.group = group;
    }

    public Column getColumnForFlattenedRow(int fieldIndex) {
        return columnsPerFlattenedField.get(fieldIndex);
    }

    @Override
    public UserTable leafMostTable() {
        assert ! tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.lastEntry().getValue();
    }

    @Override
    public UserTable rootMostTable() {
        assert ! tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.firstEntry().getValue();
    }

    @Override
    public void addColumn(IndexColumn indexColumn) {
        Table indexGenericTable = indexColumn.getColumn().getTable();
        if (!(indexGenericTable instanceof UserTable)) {
            throw new IndexColNotInGroupException (indexColumn.getIndex().getIndexName().getName(),
                    indexColumn.getColumn().getName());
        }
        UserTable indexTable = (UserTable) indexGenericTable;

        super.addColumn(indexColumn);
        GroupIndexHelper.actOnGroupIndexTables(this, indexColumn, GroupIndexHelper.ADD);

        // Add the table into our navigable map if needed. Confirm it's within the branch
        if (!tablesByDepth.values().contains(indexTable)) {
            Integer indexTableDepth = indexTable.getDepth();
            if (indexTableDepth == null) {
                throw new IndexTableNotInGroupException (indexColumn.getIndex().getIndexName().getName(),
                        indexColumn.getColumn().getName(),
                        indexTable.getName().getTableName());
            }
            Map.Entry<Integer,UserTable> rootwardEntry = tablesByDepth.floorEntry(indexTableDepth);
            Map.Entry<Integer,UserTable> leafwardEntry = tablesByDepth.ceilingEntry(indexTableDepth);
            checkIndexTableInBranch(indexColumn, indexTable, indexTableDepth, rootwardEntry, true);
            checkIndexTableInBranch(indexColumn, indexTable, indexTableDepth, leafwardEntry, false);
            tablesByDepth.put(indexTableDepth, indexTable);
        }
    }

    @Override
    protected Column indexRowCompositionColumn(HKeyColumn hKeyColumn) {
        // If we're within the branch segment, we want the root-most equivalent column, bound at the segment.
        // Otherwise (ie, rootward of the segment), we want the usual, leafward column.

        final int rootMostDepth = rootMostTable().getDepth();
        final int forTableDepth = hKeyColumn.segment().table().getDepth();

        if (forTableDepth < rootMostDepth) {
            // table is root of the branch segment; use the standard hkey column
            return hKeyColumn.column();
        }

        // table is within the branch segment
        List<Column> equivalentColumns = hKeyColumn.equivalentColumns();
        switch (getJoinType()) {
        case LEFT:
            // use a rootward bias, but no more rootward than the rootmost table
            for (Column equivalentColumn : equivalentColumns) {
                int equivalentColumnDepth = equivalentColumn.getUserTable().getDepth();
                if (equivalentColumnDepth >= rootMostDepth) {
                    return equivalentColumn;
                }
            }
            break;
        case RIGHT:
            // use a childward bias, but no more childward than the childmost table
            int leafMostDepth = leafMostTable().getDepth();
            for(ListIterator<Column> reverseCols = equivalentColumns.listIterator(equivalentColumns.size());
                reverseCols.hasPrevious();)
            {
                Column equivalentColumn = reverseCols.previous();
                int equivalentColumnDepth = equivalentColumn.getUserTable().getDepth();
                if (equivalentColumnDepth <= leafMostDepth) {
                    return equivalentColumn;
                }
            }
            break;
        }

        throw new AssertionError(
                "no suitable column found for table " + hKeyColumn.segment().table()
                        + " in " + equivalentColumns
        );
    }

    private void checkIndexTableInBranch(IndexColumn indexColumn, UserTable indexTable, int indexTableDepth,
            Map.Entry<Integer, UserTable> entry, boolean entryIsRootward)
    {
        if (entry == null) {
            return;
        }
        if (entry.getKey().intValue() == indexTableDepth) {
            throw new BranchingGroupIndexException (indexColumn.getIndex().getIndexName().getName(), 
                    indexTable.getName(), entry.getValue().getName());
        }
        UserTable entryTable = entry.getValue();

        final UserTable rootward;
        final UserTable leafward;
        if (entryIsRootward) {
            assert entry.getKey() < indexTableDepth : String.format("failed %d < %d", entry.getKey(), indexTableDepth);
            rootward = entryTable;
            leafward = indexTable;
        } else {
            assert entry.getKey() > indexTableDepth : String.format("failed %d < %d", entry.getKey(), indexTableDepth);
            rootward = indexTable;
            leafward = entryTable;
        }

        if (!leafward.isDescendantOf(rootward))
        {
            throw new BranchingGroupIndexException (indexColumn.getIndex().getIndexName().getName(), 
                    indexTable.getName(), entry.getValue().getName());
        }
    }

    @Override
    public boolean isTableIndex()
    {
        return false;
    }

    @Override
    public void computeFieldAssociations(Map<Table, Integer> ordinalMap) {
        List<UserTable> branchTables = new ArrayList<UserTable>();
        for(UserTable userTable = leafMostTable(); userTable != null; userTable = userTable.parentTable()) {
            branchTables.add(userTable);
        }
        Collections.reverse(branchTables);

        Map<UserTable,Integer> offsetsMap = new HashMap<UserTable, Integer>();
        int offset = 0;
        columnsPerFlattenedField = new ArrayList<Column>();
        for (UserTable userTable : branchTables) {
            offsetsMap.put(userTable, offset);
            offset += userTable.getColumnsIncludingInternal().size();
            columnsPerFlattenedField.addAll(userTable.getColumnsIncludingInternal());
        }
        computeFieldAssociations(ordinalMap, null, offsetsMap);
    }

    public Group getGroup()
    {
        return group;
    }

    @Override
    public HKey hKey()
    {
        return leafMostTable().hKey();
    }
    
    @SuppressWarnings("unused")
    private GroupIndex()
    {}

    private Group group;
    private final NavigableMap<Integer,UserTable> tablesByDepth = new TreeMap<Integer, UserTable>();
    private List<Column> columnsPerFlattenedField;

}
