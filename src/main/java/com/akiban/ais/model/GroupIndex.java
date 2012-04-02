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
        return tablesByDepth.lastEntry().getValue().table;
    }

    @Override
    public UserTable rootMostTable() {
        assert ! tablesByDepth.isEmpty() : "no tables participate in this group index";
        return tablesByDepth.firstEntry().getValue().table;
    }

    @Override
    public void checkMutability() {
        group.getGroupTable().checkMutability();
    }

    @Override
    public void addColumn(IndexColumn indexColumn) {
        Table indexGenericTable = indexColumn.getColumn().getTable();
        if (!(indexGenericTable instanceof UserTable)) {
            throw new IndexColNotInGroupException (indexColumn.getIndex().getIndexName().getName(),
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
            Map.Entry<Integer,ParticipatingTable> rootwardEntry = tablesByDepth.floorEntry(indexTableDepth);
            Map.Entry<Integer,ParticipatingTable> leafwardEntry = tablesByDepth.ceilingEntry(indexTableDepth);
            checkIndexTableInBranchNew(indexColumn, indexTable, indexTableDepth, rootwardEntry, true);
            checkIndexTableInBranchNew(indexColumn, indexTable, indexTableDepth, leafwardEntry, false);
            participatingTable = new ParticipatingTable(indexTable);
            tablesByDepth.put(indexTableDepth, participatingTable);
        } else if (participatingTable.table != indexTable) {
            throw new BranchingGroupIndexException(indexColumn.getIndex().getIndexName().getName(),
                                                   indexTable.getName(),
                                                   participatingTable.table.getName());
        }
        participatingTable.markInvolvedInIndex(indexColumn.getColumn());
    }

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
        } else {
            // TODO: Can index maintenance be skipped in this case?
            return true;
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

    private void checkIndexTableInBranchNew(IndexColumn indexColumn, UserTable indexTable, int indexTableDepth,
            Map.Entry<Integer, ParticipatingTable> entry, boolean entryIsRootward)
    {
        if (entry == null) {
            return;
        }
        if (entry.getKey().intValue() == indexTableDepth) {
            throw new BranchingGroupIndexException (indexColumn.getIndex().getIndexName().getName(), 
                    indexTable.getName(), entry.getValue().table.getName());
        }
        UserTable entryTable = entry.getValue().table;

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
                    indexTable.getName(), entry.getValue().table.getName());
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
        // Complete computation of inIndex bitsets
        for (ParticipatingTable participatingTable : tablesByDepth.values()) {
            participatingTable.close();
        }
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
    
    private final Group group;
    private final NavigableMap<Integer,ParticipatingTable> tablesByDepth = new TreeMap<Integer, ParticipatingTable>();
    private List<Column> columnsPerFlattenedField;

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
