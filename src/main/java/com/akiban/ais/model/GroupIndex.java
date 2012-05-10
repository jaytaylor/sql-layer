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

package com.akiban.ais.model;

import com.akiban.server.error.BranchingGroupIndexException;
import com.akiban.server.error.IndexColNotInGroupException;

import java.util.*;

public class GroupIndex extends Index
{
    // Index interface

    @Override
    public UserTable leafMostTable()
    {
        assert !tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.lastEntry().getValue().table;
    }

    @Override
    public UserTable rootMostTable()
    {
        assert !tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.firstEntry().getValue().table;
    }

    @Override
    public void checkMutability()
    {
        group.getGroupTable().checkMutability();
    }

    @Override
    public void addColumn(IndexColumn indexColumn)
    {
        Table indexGenericTable = indexColumn.getColumn().getTable();
        if (!(indexGenericTable instanceof UserTable)) {
            throw new IndexColNotInGroupException(indexColumn.getIndex().getIndexName().getName(),
                                                  indexColumn.getColumn().getName());
        }
        UserTable indexTable = (UserTable) indexGenericTable;
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
        List<UserTable> branchTables = new ArrayList<UserTable>();
        for (UserTable userTable = leafMostTable(); userTable != null; userTable = userTable.parentTable()) {
            branchTables.add(userTable);
        }
        Collections.reverse(branchTables);

        Map<UserTable, Integer> offsetsMap = new HashMap<UserTable, Integer>();
        int offset = 0;
        columnsPerFlattenedField = new ArrayList<Column>();
        for (UserTable userTable : branchTables) {
            offsetsMap.put(userTable, offset);
            offset += userTable.getColumnsIncludingInternal().size();
            columnsPerFlattenedField.addAll(userTable.getColumnsIncludingInternal());
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
    public boolean columnsOverlap(UserTable table, BitSet modifiedColumnPositions)
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

    public static GroupIndex create(AkibanInformationSchema ais, Group group, String indexName, Integer indexId,
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

    public GroupIndex(Group group,
                      String indexName,
                      Integer indexId,
                      Boolean isUnique,
                      String constraint,
                      JoinType joinType)
    {
        // index checks index name.
        super(new TableName("", group.getName()), indexName, indexId, isUnique, constraint, joinType, true);
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

    // For use by this class

    private void computeFieldAssociations(Map<Table, Integer> ordinalMap,
                                          Map<? extends Table, Integer> flattenedRowOffsets)
    {
        freezeColumns();
        AssociationBuilder toIndexRowBuilder = new AssociationBuilder();
        List<Column> indexColumns = new ArrayList<Column>();
        // Add index key fields
        for (IndexColumn iColumn : getKeyColumns()) {
            Column column = iColumn.getColumn();
            indexColumns.add(column);
            toIndexRowBuilder.rowCompEntry(columnPosition(flattenedRowOffsets, column), -1);
        }
        // Add leafward-biased hkey fields not already included
        int indexColumnPosition = indexColumns.size();
        int indexRootDepth = rootMostTable().getDepth();
        int indexLeafDepth = leafMostTable().getDepth();
        List<IndexColumn> leafwardHKeyColumns = new ArrayList<IndexColumn>();
        List<Column> rootwardColumns = new ArrayList<Column>();
        HKey hKey = hKey();
        for (HKeySegment hKeySegment : hKey.segments()) {
            Integer ordinal = ordinalMap.get(hKeySegment.table());
            assert ordinal != null : hKeySegment.table();
            for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                Column leafwardColumn = hKeyColumn.column();
                // Get rootward columns among the tables covered by the index: equivalent columns minus
                // leafwardColumn, that aren't already included elsewhere.
                for (Column equivalentColumn : hKeyColumn.equivalentColumns()) {
                    int equivalentColumnTableDepth = equivalentColumn.getUserTable().getDepth();
                    if (equivalentColumnTableDepth >= indexRootDepth &&
                        equivalentColumnTableDepth <= indexLeafDepth &&
                        equivalentColumn != leafwardColumn &&
                        !indexColumns.contains(equivalentColumn) &&
                        !rootwardColumns.contains(equivalentColumn)) {
                        rootwardColumns.add(equivalentColumn);
                    }
                }
                if (!indexColumns.contains(leafwardColumn)) {
                    toIndexRowBuilder.rowCompEntry(columnPosition(flattenedRowOffsets, leafwardColumn), -1);
                    indexColumns.add(leafwardColumn);
                    leafwardHKeyColumns.add(new IndexColumn(this, leafwardColumn, indexColumnPosition++, true, 0));
                }
            }
        }
        // Complete metadata for rootward-biased hkey fields
        List<IndexColumn> rootwardHKeyColumns = new ArrayList<IndexColumn>();
        for (Column column : rootwardColumns) {
            IndexColumn indexColumn = new IndexColumn(this, column, indexColumnPosition++, true, 0);
            rootwardHKeyColumns.add(indexColumn);
            toIndexRowBuilder.rowCompEntry(columnPosition(flattenedRowOffsets, column), -1);
        }
        allColumns = new ArrayList<IndexColumn>();
        allColumns.addAll(keyColumns);
        allColumns.addAll(leafwardHKeyColumns);
        allColumns.addAll(rootwardHKeyColumns);
        indexRowComposition = toIndexRowBuilder.createIndexRowComposition();
        computeHKeyDerivations(ordinalMap);
    }

    private void computeHKeyDerivations(Map<Table, Integer> ordinalMap)
    {
        indexToHKeys = new IndexToHKey[leafMostTable().getDepth() + 1];
        UserTable table = leafMostTable();
        while (table != null) {
            int tableDepth = table.getDepth();
            assert tableDepth <= leafMostTable().getDepth() : table;
            AssociationBuilder hKeyBuilder = new AssociationBuilder();
            HKey hKey = table.hKey();
            for (HKeySegment hKeySegment : hKey.segments()) {
                hKeyBuilder.toHKeyEntry(ordinalMap.get(hKeySegment.table()), -1);
                for (HKeyColumn hKeyColumn : hKeySegment.columns()) {
                    Column column = hKeyColumn.column();
                    int indexColumnPosition = positionOf(column);
                    if (indexColumnPosition == -1) {
                        for (Column equivalentColumn : hKeyColumn.equivalentColumns()) {
                            int equivalentColumnPosition = positionOf(equivalentColumn);
                            if (indexColumnPosition == -1 && equivalentColumnPosition != -1) {
                                indexColumnPosition = equivalentColumnPosition;
                            }
                        }
                        assert indexColumnPosition != -1 : column;
                    }
                    hKeyBuilder.toHKeyEntry(-1, indexColumnPosition);
                }
            }
            indexToHKeys[tableDepth] = hKeyBuilder.createIndexToHKey();
            table = table.parentTable();
        }
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

    private boolean indexCovers(UserTable table)
    {
        return table.getDepth() >= rootMostTable().getDepth() && table.getDepth() <= leafMostTable().getDepth();
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

    private void checkIndexTableInBranchNew(IndexColumn indexColumn, UserTable indexTable, int indexTableDepth,
                                            Map.Entry<Integer, ParticipatingTable> entry, boolean entryIsRootward)
    {
        if (entry == null) {
            return;
        }
        if (entry.getKey() == indexTableDepth) {
            throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                   indexTable.getName(), entry.getValue().table.getName());
        }
        UserTable entryTable = entry.getValue().table;

        final UserTable rootward;
        final UserTable leafward;
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
    private final NavigableMap<Integer, ParticipatingTable> tablesByDepth = new TreeMap<Integer, ParticipatingTable>();
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

        public ParticipatingTable(UserTable table)
        {
            this.table = table;
            this.inIndex = new BitSet(table.getColumnsIncludingInternal().size());
        }

        // The table participating in the group index
        final UserTable table;
        // The columns of the table that contribute to the group index key or value. This includes PK columns,
        // FK columns, and any columns declared in the key. The PK and FK columns may not always be necessary, as
        // the logic here does not account for whether the index includes the leafward or rootward side of an FK.
        // As a result, we may decide to do index maintenance when it could otherwise be safely avoided.
        final BitSet inIndex;
    }
}
