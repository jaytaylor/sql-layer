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

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class GroupIndex extends Index
{
    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, String constraint)
    {
        GroupIndex index = new GroupIndex(group, indexName, indexId, isUnique, constraint);
        group.addIndex(index);
        return index;
    }

    public GroupIndex(Group group, String indexName, Integer indexId, Boolean isUnique, String constraint)
    {
        super(new TableName("", group.getName()), indexName, indexId, isUnique, constraint);
        this.group = group;
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
            throw new GroupIndexCreationException("index column must be of user table: " + indexColumn);
        }
        UserTable indexTable = (UserTable) indexGenericTable;

        super.addColumn(indexColumn);
        GroupIndexHelper.actOnGroupIndexTables(this, indexColumn, GroupIndexHelper.ADD);

        // Add the table into our navigable map if needed. Confirm it's within the branch
        if (!tablesByDepth.values().contains(indexTable)) {
            Integer indexTableDepth = indexTable.getDepth();
            if (indexTableDepth == null) {
                throw new GroupIndexCreationException("index table not in group: " + indexTable);
            }
            Map.Entry<Integer,UserTable> rootwardEntry = tablesByDepth.floorEntry(indexTableDepth);
            Map.Entry<Integer,UserTable> leafwardEntry = tablesByDepth.ceilingEntry(indexTableDepth);
            checkIndexTableInBranch(indexColumn, indexTable, indexTableDepth, rootwardEntry, true);
            checkIndexTableInBranch(indexColumn, indexTable, indexTableDepth, leafwardEntry, false);
            tablesByDepth.put(indexTableDepth, indexTable);
        }
    }

    private void checkIndexTableInBranch(IndexColumn indexColumn, UserTable indexTable, int indexTableDepth,
            Map.Entry<Integer, UserTable> entry, boolean entryIsRootward)
    {
        if (entry == null) {
            return;
        }
        if (entry.getKey().intValue() == indexTableDepth) {
            throw new GroupIndexCreationException(
                    indexTable + " and " + entry.getValue() + " must be multibranch; both have depth " + entry.getKey()
            );
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
            throw new GroupIndexCreationException(indexColumn + " is not within this group index's branch");
        }
    }

    @Override
    public boolean isTableIndex()
    {
        return false;
    }

    @Override
    public void computeFieldAssociations(Map<Table, Integer> ordinalMap) {
        throw new UnsupportedOperationException("Implement");
        // Probably something like:
        //internalComputeFieldAssociations(ordinalMap, leftMostTable);
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
    {

    }

    private Group group;
    private final NavigableMap<Integer,UserTable> tablesByDepth = new TreeMap<Integer, UserTable>();

    // nested classes
    public static class GroupIndexCreationException extends RuntimeException {
        public GroupIndexCreationException(String message) {
            super(message);
        }
    }
}
