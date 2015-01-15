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

package com.foundationdb.ais.model;

import com.foundationdb.ais.model.validation.AISInvariants;
import com.foundationdb.server.error.BranchingGroupIndexException;

import java.util.*;

public class GroupIndex extends Index
{
    // Index interface

    @Override
    public Table leafMostTable()
    {
        assert !tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.lastEntry().getValue().table;
    }

    @Override
    public Table rootMostTable()
    {
        assert !tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.firstEntry().getValue().table;
    }

    @Override
    public void checkMutability()
    {
        group.getRoot().checkMutability();
    }

    @Override
    public Collection<Integer> getAllTableIDs()
    {
        List<Integer> branchIDs = new ArrayList<>(tablesByDepth.size());
        for (Table table = leafMostTable(); table != null; table = table.getParentTable()) {
            branchIDs.add(table.getTableId());
        }
        return branchIDs;
    }

    @Override
    public void addColumn(IndexColumn indexColumn)
    {
        Table indexTable = indexColumn.getColumn().getTable();
        Integer indexTableDepth = indexTable.getDepth();
        assert indexTableDepth != null;

        super.addColumn(indexColumn);
        GroupIndexHelper.actOnGroupIndexTables(this, indexColumn, GroupIndexHelper.ADD);

        // Add the table into our navigable map if needed. Confirm it's within the branch
        ParticipatingTable participatingTable = tablesByDepth.get(indexTableDepth);
        if (participatingTable == null) {
            Map.Entry<Integer, ParticipatingTable> rootwardEntry = tablesByDepth.floorEntry(indexTableDepth);
            Map.Entry<Integer, ParticipatingTable> leafwardEntry = tablesByDepth.ceilingEntry(indexTableDepth);
            checkIndexTableInBranchNew(indexColumn, indexTable, indexTableDepth, rootwardEntry, true);
            checkIndexTableInBranchNew(indexColumn, indexTable, indexTableDepth, leafwardEntry, false);
            participatingTable = new ParticipatingTable(indexTable);
            tablesByDepth.put(indexTableDepth, participatingTable);
        }
        else if (participatingTable.table != indexTable) {
            throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                   indexTable.getName(),
                                                   participatingTable.table.getName());
        }
        participatingTable.markInvolvedInIndex(indexColumn.getColumn());
    }

    @Override
    public boolean isTableIndex()
    {
        return false;
    }

    @Override
    public void computeFieldAssociations(Map<Table, Integer> ordinalMap)
    {
        List<Table> branchTables = new ArrayList<>();
        for (Table table = leafMostTable(); table != null; table = table.getParentTable()) {
            branchTables.add(table);
        }
        Collections.reverse(branchTables);

        Map<Table, Integer> offsetsMap = new HashMap<>();
        int offset = 0;
        columnsPerFlattenedField = new ArrayList<>();
        for (Table table : branchTables) {
            offsetsMap.put(table, offset);
            offset += table.getColumnsIncludingInternal().size();
            columnsPerFlattenedField.addAll(table.getColumnsIncludingInternal());
        }
        computeFieldAssociations(ordinalMap, offsetsMap);
        // Complete computation of inIndex bitsets
        for (ParticipatingTable participatingTable : tablesByDepth.values()) {
            participatingTable.close();
        }
    }

    @Override
    public HKey hKey()
    {
        return leafMostTable().hKey();
    }

    // GroupIndex interface

    // A row of the given table is being changed in the columns described by modifiedColumnPositions.
    // Return true iff there are any columns in common with those columns of the table contributing to the
    // index. A result of false means that the row change need not result in group index maintenance.
    public boolean columnsOverlap(Table table, BitSet modifiedColumnPositions)
    {
        ParticipatingTable participatingTable = tablesByDepth.get(table.getDepth());
        if (participatingTable != null) {
            assert participatingTable.table == table;
            int n = modifiedColumnPositions.length();
            for (int i = 0; i < n; i++) {
                if (modifiedColumnPositions.get(i) && participatingTable.inIndex.get(i)) {
                    return true;
                }
            }
            return false;
        }
        else {
            // TODO: Can index maintenance be skipped in this case?
            return true;
        }
    }

    public Group getGroup()
    {
        return group;
    }

    public static GroupIndex create(AkibanInformationSchema ais, Group group, GroupIndex index)
    {
        GroupIndex copy = create(ais, group, index.getIndexName().getName(), index.getIndexId(),
                                 index.isUnique(), index.isPrimaryKey(), index.getJoinType());
        if (index.isSpatial()) {
            copy.markSpatial(index.firstSpatialArgument(), index.spatialColumns(), index.functionName());
        }
        return copy;
    }

    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, Boolean isPrimary, JoinType joinType) {
        return create(ais, group, indexName, indexId, isUnique, isPrimary, null, joinType);

    }

    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
                                    Boolean isUnique, Boolean isPrimary, TableName constraintName, JoinType joinType)
    {
        ais.checkMutability();
        if(constraintName != null) {
            throw new IllegalArgumentException("Group indexes are never constraints");
        }
        AISInvariants.checkDuplicateIndexesInGroup(group, indexName);
        GroupIndex index = new GroupIndex(group, indexName, indexId, isUnique, isPrimary, joinType);
        group.addIndex(index);
        return index;
    }

    private GroupIndex(Group group,
                      String indexName,
                      Integer indexId,
                      Boolean isUnique,
                      Boolean isPrimary,
                      JoinType joinType)
    {
        super(group.getName(), indexName, indexId, isUnique, isPrimary, null, joinType);
        this.group = group;
    }

    public Column getColumnForFlattenedRow(int fieldIndex)
    {
        return columnsPerFlattenedField.get(fieldIndex);
    }

    public IndexToHKey indexToHKey(int tableDepth)
    {
        if (tableDepth > leafMostTable().getDepth()) {
            throw new IllegalArgumentException(Integer.toString(tableDepth));
        }
        return indexToHKeys[tableDepth];
    }

    public void disassociate() {
        GroupIndexHelper.actOnGroupIndexTables(this, GroupIndexHelper.REMOVE);
    }

    // For use by this class

    private void computeFieldAssociations(Map<Table, Integer> ordinalMap,
                                          Map<? extends Table, Integer> flattenedRowOffsets)
    {
        freezeColumns();
        allColumns = new ArrayList<>();
        allColumns.addAll(keyColumns);
        AssociationBuilder toIndexRowBuilder = new AssociationBuilder();
        List<Column> indexColumns = new ArrayList<>();
        // Add index key fields
        for (IndexColumn iColumn : getKeyColumns()) {
            Column column = iColumn.getColumn();
            indexColumns.add(column);
            toIndexRowBuilder.rowCompEntry(columnPosition(flattenedRowOffsets, column), -1);
        }
        // Add hkey fields not already included
        int indexColumnPosition = indexColumns.size();
        HKey hKey = hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            Integer ordinal = ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column undeclaredHKeyColumn = undeclaredHKeyColumn(hKeyColumn);
                if (!indexColumns.contains(undeclaredHKeyColumn)) {
                    toIndexRowBuilder.rowCompEntry(columnPosition(flattenedRowOffsets, undeclaredHKeyColumn), -1);
                    indexColumns.add(undeclaredHKeyColumn);
                    allColumns.add(new IndexColumn(this, undeclaredHKeyColumn, indexColumnPosition++, true, 0));
                }
            }
        }
        indexRowComposition = toIndexRowBuilder.createIndexRowComposition();
        computeHKeyDerivations(ordinalMap);
    }

    private void computeHKeyDerivations(Map<Table, Integer> ordinalMap)
    {
        indexToHKeys = new IndexToHKey[leafMostTable().getDepth() + 1];
        Table table = leafMostTable();
        while (table != null) {
            int tableDepth = table.getDepth();
            assert tableDepth <= leafMostTable().getDepth() : table;
            AssociationBuilder hKeyBuilder = new AssociationBuilder();
            HKey hKey = table.hKey();
            for (HKeySegment hKeySegment : hKey.segments()) {
                hKeyBuilder.toHKeyEntry(ordinalMap.get(hKeySegment.table()), -1);
                for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                    int indexColumnPosition = positionOf(hKeyColumn.column());
                    if (indexColumnPosition == -1) {
                        indexColumnPosition = substituteHKeyColumnPosition(hKeyColumn);
                    }
                    hKeyBuilder.toHKeyEntry(-1, indexColumnPosition);
                }
            }
            indexToHKeys[tableDepth] = hKeyBuilder.createIndexToHKey();
            table = table.getParentTable();
        }
    }

    private Column undeclaredHKeyColumn(HKeyColumn hKeyColumn)
    {
        Column undeclaredHKeyColumn = null;
        int rootMostDepth = rootMostTable().getDepth();
        List<Column> equivalentColumns = hKeyColumn.equivalentColumns();
        switch (getJoinType()) {
            case LEFT:
                // use a rootward bias, but no more rootward than the rootmost table
                for (Column equivalentColumn : equivalentColumns) {
                    int equivalentColumnDepth = equivalentColumn.getTable().getDepth();
                    if (undeclaredHKeyColumn == null && equivalentColumnDepth >= rootMostDepth) {
                        undeclaredHKeyColumn = equivalentColumn;
                    }
                }
                break;
            case RIGHT:
                // use a leafward bias, but no more leafward than the leafdmost table
                int leafMostDepth = leafMostTable().getDepth();
                for(ListIterator<Column> reverseCols = equivalentColumns.listIterator(equivalentColumns.size());
                    reverseCols.hasPrevious();)
                {
                    Column equivalentColumn = reverseCols.previous();
                    int equivalentColumnDepth = equivalentColumn.getTable().getDepth();
                    if (undeclaredHKeyColumn == null && equivalentColumnDepth <= leafMostDepth) {
                        undeclaredHKeyColumn = equivalentColumn;
                    }
                }
                break;
        }
        if (undeclaredHKeyColumn == null) {
            undeclaredHKeyColumn = hKeyColumn.column();
        }
        return undeclaredHKeyColumn;
    }

    private int substituteHKeyColumnPosition(HKeyColumn hKeyColumn)
    {
        int substituteHKeyColumnPosition = -1;
        // Given an hkey row, we need to construct an hkey for some table, either a table covered by the index
        // or some ancestor table. hKeyColumn is an hkey column of that table. If we're here, then
        // hKeyColumn.column() cannot be obtained from the index row itself, so the question is: which of the
        // hKeyColumn's equivalent columns should be used.
        // - If the hkey column is above the root: Use the rootmost equivalent column of the hkey columns in the index
        //   row.
        // - Otherwise the hkey column belongs to a table covered by the index.
        //     - For a left join index, use the nearest rootward equivalent column.
        //     - For a right join index, use the nearest leafward equivalent column.
        List<Column> equivalentColumns = hKeyColumn.equivalentColumns(); // sorted by depth, root first
        Integer targetTableDepth = hKeyColumn.column().getTable().getDepth();
        if (targetTableDepth < rootMostTable().getDepth()) {
            for (int i = 0; substituteHKeyColumnPosition == -1 && i < equivalentColumns.size(); i++) {
                Column equivalentColumn = equivalentColumns.get(i);
                substituteHKeyColumnPosition = positionOf(equivalentColumn);
            }
        } else {
            switch (getJoinType()) {
                case LEFT:
                    for (int i = 0; i < equivalentColumns.size(); i++) {
                        Column equivalentColumn = equivalentColumns.get(i);
                        int equivalentColumnPosition = positionOf(equivalentColumn);
                        if (equivalentColumnPosition != -1 && depth(equivalentColumn) < targetTableDepth) {
                            substituteHKeyColumnPosition = equivalentColumnPosition;
                        }
                    }
                    break;
                case RIGHT:
                    for (int i = equivalentColumns.size() - 1; i >= 0; i--) {
                        Column equivalentColumn = equivalentColumns.get(i);
                        int equivalentColumnPosition = positionOf(equivalentColumn);
                        if (equivalentColumnPosition != -1 && depth(equivalentColumn) > targetTableDepth) {
                            substituteHKeyColumnPosition = equivalentColumnPosition;
                        }
                    }
                    break;
            }
        }
        assert substituteHKeyColumnPosition != -1 : hKeyColumn;
        return substituteHKeyColumnPosition;
    }

    private int depth(Column column)
    {
        return column.getTable().getDepth();
    }

    private int positionOf(Column column)
    {
        for (IndexColumn indexColumn : allColumns) {
            if (indexColumn.getColumn() == column) {
                return indexColumn.getPosition();
            }
        }
        return -1;
    }

    private static int columnPosition(Map<? extends Table, Integer> flattenedRowOffsets, Column column)
    {
        int position = column.getPosition();
        Integer offset = flattenedRowOffsets.get(column.getTable());
        if (offset == null) {
            throw new NullPointerException("no offset for " + column.getTable() + " in " + flattenedRowOffsets);
        }
        position += offset;
        return position;
    }

    private void checkIndexTableInBranchNew(IndexColumn indexColumn, Table indexTable, int indexTableDepth,
                                            Map.Entry<Integer, ParticipatingTable> entry, boolean entryIsRootward)
    {
        if (entry == null) {
            return;
        }
        if (entry.getKey() == indexTableDepth) {
            throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                   indexTable.getName(), entry.getValue().table.getName());
        }
        Table entryTable = entry.getValue().table;

        final Table rootward;
        final Table leafward;
        if (entryIsRootward) {
            assert entry.getKey() < indexTableDepth : String.format("failed %d < %d", entry.getKey(), indexTableDepth);
            rootward = entryTable;
            leafward = indexTable;
        }
        else {
            assert entry.getKey() > indexTableDepth : String.format("failed %d < %d", entry.getKey(), indexTableDepth);
            rootward = indexTable;
            leafward = entryTable;
        }

        if (!leafward.isDescendantOf(rootward)) {
            throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                   indexTable.getName(), entry.getValue().table.getName());
        }
    }

    // Object state

    private final Group group;
    private final NavigableMap<Integer, ParticipatingTable> tablesByDepth = new TreeMap<>();
    private List<Column> columnsPerFlattenedField;
    private IndexToHKey[] indexToHKeys;

    // Inner classes

    private static class ParticipatingTable
    {
        public void markInvolvedInIndex(Column column)
        {
            assert column.getTable() == table;
            inIndex.set(column.getPosition(), true);
        }

        public void close()
        {
            for (Column pkColumn : table.getPrimaryKeyIncludingInternal().getColumns()) {
                inIndex.set(pkColumn.getPosition(), true);
            }
            if (table.getParentJoin() != null) {
                for (JoinColumn joinColumn : table.getParentJoin().getJoinColumns()) {
                    inIndex.set(joinColumn.getChild().getPosition(), true);
                }
            }
        }

        public ParticipatingTable(Table table)
        {
            this.table = table;
            this.inIndex = new BitSet(table.getColumnsIncludingInternal().size());
        }

        // The table participating in the group index
        final Table table;
        // The columns of the table that contribute to the group index key or value. This includes PK columns,
        // FK columns, and any columns declared in the key. The PK and FK columns may not always be necessary, as
        // the logic here does not account for whether the index includes the leafward or rootward side of an FK.
        // As a result, we may decide to do index maintenance when it could otherwise be safely avoided.
        final BitSet inIndex;
    }
}
