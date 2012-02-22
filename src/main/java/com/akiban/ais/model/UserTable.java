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

import com.akiban.util.ArgumentValidation;

import java.util.*;

public class UserTable extends Table
{
    public static UserTable create(AkibanInformationSchema ais,
                                   String schemaName,
                                   String tableName,
                                   Integer tableId)
    {
        
        UserTable userTable = new UserTable(ais, schemaName, tableName, tableId);
        ais.addUserTable(userTable);
        return userTable;
    }

    public UserTable(AkibanInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        super(ais, schemaName, tableName, tableId);
        migrationUsage = MigrationUsage.AKIBAN_STANDARD;
    }

    @Override
    public boolean isUserTable()
    {
        return true;
    }

    @Override
    protected void addIndex(TableIndex index)
    {
        super.addIndex(index);
        if (index.isPrimaryKey()) {
            assert primaryKey == null;
            primaryKey = new PrimaryKey(index);
        }
    }
    
    /**
    * Returns the columns in this table that are constrained to match the given column, e.g.
     * customer.cid and order.cid. These will be ordered by the table they appear on, root to leaf.
     * The given column will itself be in the resulting list. The list is calculated anew each time
     * and may be modified as needed by the caller.
     * @param column the column for which to find matching columns.
     * @return a new list of columns equivalent to the given column, including that column itself.
     */
    List<Column> matchingColumns(Column column)
    {
        // TODO: make this a AISValidation check
        ArgumentValidation.isTrue(column + " doesn't belong to " + getName(), column.getTable() == this);
        List<Column> matchingColumns = new ArrayList<Column>();
        matchingColumns.add(column);
        findMatchingAncestorColumns(column, matchingColumns);
        findMatchingDescendantColumns(column, matchingColumns);
        Collections.sort(matchingColumns, COLUMNS_BY_TABLE_DEPTH);
        return matchingColumns;
    }

    private void findMatchingAncestorColumns(Column fromColumn, List<Column> matchingColumns)
    {
        Join join = ((UserTable)fromColumn.getTable()).getParentJoin();
        if (join != null) {
            JoinColumn ancestorJoinColumn = null;
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                if (joinColumn.getChild() == fromColumn) {
                    ancestorJoinColumn = joinColumn;
                }
            }
            if (ancestorJoinColumn != null) {
                Column ancestorColumn = ancestorJoinColumn.getParent();
                matchingColumns.add(ancestorColumn);
                findMatchingAncestorColumns(ancestorJoinColumn.getParent(), matchingColumns);
            }
        }
    }

    private void findMatchingDescendantColumns(Column fromColumn, List<Column> matchingColumns)
    {
        for (Join join : getChildJoins()) {
            JoinColumn descendantJoinColumn = null;
            for (JoinColumn joinColumn : join.getJoinColumns()) {
                if (joinColumn.getParent() == fromColumn) {
                    descendantJoinColumn = joinColumn;
                }
            }
            if (descendantJoinColumn != null) {
                Column descendantColumn = descendantJoinColumn.getChild();
                matchingColumns.add(descendantColumn);
                findMatchingDescendantColumns(descendantJoinColumn.getChild(), matchingColumns);
            }
        }
    }

    public void setSize(int size)
    {
        this.size = size;
    }

    public int getSize()
    {
        return size;
    }

    public void addCandidateParentJoin(Join parentJoin)
    {
        candidateParentJoins.add(parentJoin);
    }

    public void addCandidateChildJoin(Join childJoin)
    {
        candidateChildJoins.add(childJoin);
    }

    public List<Join> getCandidateParentJoins()
    {
        return Collections.unmodifiableList(candidateParentJoins);
    }

    public List<Join> getCandidateChildJoins()
    {
        return Collections.unmodifiableList(candidateChildJoins);
    }

    public Join getParentJoin()
    {
        Join parentJoin = null;
        Group group = getGroup();
        if (group != null) {
            for (Join candidateParentJoin : candidateParentJoins) {
                if (candidateParentJoin.getGroup() == group) {
                    parentJoin = candidateParentJoin;
                }
            }
        }
        return parentJoin;
    }

    public List<Join> getChildJoins()
    {
        List<Join> childJoins = new ArrayList<Join>();
        Group group = getGroup();
        if (group != null) {
            for (Join candidateChildJoin : candidateChildJoins) {
                if (candidateChildJoin.getGroup() == group) {
                    childJoins.add(candidateChildJoin);
                }
            }
        }
        return childJoins;
    }

    public Column getAutoIncrementColumn()
    {
        Column autoIncrementColumn = null;
        for (Column column : getColumns()) {
            if (column.getInitialAutoIncrementValue() != null) {
                autoIncrementColumn = column;
            }
        }
        return autoIncrementColumn;
    }

    @Override
    public Collection<TableIndex> getIndexes()
    {
        Collection<TableIndex> indexes = super.getIndexes();
        return removeInternalColumnIndexes(indexes);
    }

    public Collection<TableIndex> getIndexesIncludingInternal()
    {
        return super.getIndexes();
    }

    @Override
    public TableIndex getIndex(String indexName)
    {
        TableIndex index = null;
        if (indexName.equals(Index.PRIMARY_KEY_CONSTRAINT)) {
            // getPrimaryKey has logic for handling Akiban PK
            PrimaryKey primaryKey = getPrimaryKey();
            index = primaryKey == null ? null : primaryKey.getIndex();
        } else {
            index = super.getIndex(indexName);
        }
        return index;
    }

    public boolean isDescendantOf(UserTable other) {
        if (getGroup() == null || !getGroup().equals(other.getGroup())) {
            return false;
        }
        UserTable possibleDescendant = this;
        while (possibleDescendant != null) {
            if (possibleDescendant.equals(other)) {
                return true;
            }
            possibleDescendant = possibleDescendant.parentTable();
        }
        return false;
    }

    public Index getIndexIncludingInternal(String indexName)
    {
        return super.getIndex(indexName);
    }

    @Override
    public void traversePreOrder(Visitor visitor)
    {
        for (Column column : getColumns()) {
            visitor.visitColumn(column);
        }
        for (Index index : getIndexes()) {
            visitor.visitIndex(index);
            index.traversePreOrder(visitor);
        }
    }

    @Override
    public void traversePostOrder(Visitor visitor)
    {
        for (Column column : getColumns()) {
            visitor.visitColumn(column);
        }
        for (Index index : getIndexes()) {
            index.traversePostOrder(visitor);
            visitor.visitIndex(index);
        }
    }

    public void setInitialAutoIncrementValue(Long initialAutoIncrementValue)
    {
        for (Column column : getColumns()) {
            if (column.getInitialAutoIncrementValue() != null) {
                column.setInitialAutoIncrementValue(initialAutoIncrementValue);
            }
        }
    }

    public synchronized PrimaryKey getPrimaryKey()
    {
        PrimaryKey declaredPrimaryKey = primaryKey;
        if (declaredPrimaryKey != null) {
            List<IndexColumn> pkColumns = primaryKey.getIndex().getKeyColumns();
            if (pkColumns.size() == 1 && pkColumns.get(0).getColumn().isAkibanPKColumn()) {
                declaredPrimaryKey = null;
            }
        }
        return declaredPrimaryKey;
    }

    public synchronized PrimaryKey getPrimaryKeyIncludingInternal()
    {
        return primaryKey;
    }

    public synchronized void endTable()
    {
        // Creates a PK for a pk-less table.
        if (primaryKey == null) {
            // Find primary key index
            TableIndex primaryKeyIndex = null;
            for (TableIndex index : getIndexesIncludingInternal()) {
                if (index.isPrimaryKey()) {
                    primaryKeyIndex = index;
                }
            }
            if (primaryKeyIndex == null) {
                primaryKeyIndex = createAkibanPrimaryKeyIndex();
            }
            assert primaryKeyIndex != null : this;
            primaryKey = new PrimaryKey(primaryKeyIndex);
        }
    }

    public synchronized Integer getDepth()
    {
        if (depth == null && getGroup() != null) {
            depth = getParentJoin() == null ? 0 : getParentJoin().getParent().getDepth() + 1;
        }
        return depth;
    }

    public Boolean isLookupTable()
    {
        return migrationUsage == MigrationUsage.AKIBAN_LOOKUP_TABLE;
    }

    public Boolean isRoot()
    {
        return getGroup() == null || getParentJoin() == null;
    }

    public void setLookupTable(Boolean isLookup)
    {
        setMigrationUsage(isLookup ? MigrationUsage.AKIBAN_LOOKUP_TABLE : MigrationUsage.AKIBAN_STANDARD);
    }

    public MigrationUsage getMigrationUsage()
    {
        return migrationUsage;
    }

    public void setMigrationUsage(MigrationUsage migrationUsage)
    {
        assert (this.migrationUsage != MigrationUsage.INCOMPATIBLE || migrationUsage == MigrationUsage.INCOMPATIBLE)
            : "cannot change migration usage from INCOMPATIBLE to " + migrationUsage;
        this.migrationUsage = migrationUsage;
    }

    public void setEngine(String engine)
    {
        this.engine = engine;
    }

    public HKey hKey()
    {
        assert getGroup() != null;
        if (hKey == null) {
            computeHKey();
        }
        return hKey;
    }

    // An HKey in terms of group table columns, for a branch of a group, terminating with this user table.
    public HKey branchHKey()
    {
        if (branchHKey == null) {
            // Construct an hkey in which group columns replace user columns.
            branchHKey = new HKey(this);
            for (HKeySegment userHKeySegment : hKey().segments()) {
                HKeySegment branchHKeySegment = branchHKey.addSegment(userHKeySegment.table());
                for (HKeyColumn userHKeyColumn : userHKeySegment.columns()) {
                    branchHKeySegment.addColumn(userHKeyColumn.column().getGroupColumn());
                }
            }
        }
        return branchHKey;
    }

    public List<Column> allHKeyColumns()
    {
        assert getGroup() != null;
        assert getPrimaryKeyIncludingInternal() != null;
        if (allHKeyColumns == null) {
            allHKeyColumns = new ArrayList<Column>();
            for (HKeySegment segment : hKey().segments()) {
                for (HKeyColumn hKeyColumn : segment.columns()) {
                    allHKeyColumns.add(hKeyColumn.column());
                }
            }
        }
        return allHKeyColumns;
    }

    public boolean containsOwnHKey()
    {
        hKey(); // Ensure hKey and containsOwnHKey are computed
        return containsOwnHKey;
    }

    public UserTable parentTable()
    {
        Join join = getParentJoin();
        return join == null ? null : join.getParent();
    }

    // Descendent tables whose hkeys are affected by a change to this table's PK or FK.
    public List<UserTable> hKeyDependentTables()
    {
        if (hKeyDependentTables == null) {
            synchronized (lazyEvaluationLock) {
                if (hKeyDependentTables == null) {
                    hKeyDependentTables = new ArrayList<UserTable>();
                    for (Join join : getChildJoins()) {
                        UserTable child = join.getChild();
                        if (!child.containsOwnHKey()) {
                            addTableAndDescendents(child, hKeyDependentTables);
                        }
                    }
                }
            }
        }
        return hKeyDependentTables;
    }
    
    private void addTableAndDescendents(UserTable table, List<UserTable> accumulator)
    {
        accumulator.add(table);
        for (Join join : table.getChildJoins()) {
            addTableAndDescendents(join.getChild(), accumulator);
        }
    }
    
    @SuppressWarnings("unused")
    private UserTable()
    {
        // XXX: GWT requires empty constructor
    }

    private void computeHKey()
    {
        hKey = new HKey(this);
        List<Column> hKeyColumns = new ArrayList<Column>();
        if (!isRoot()) {
            // Start with the parent's hkey
            Join join = getParentJoin();
            HKey parentHKey = join.getParent().hKey();
            // Start forming this table's full hkey by including all of the parent hkey columns, but replacing
            // columns participating in the join (to this table) by columns from this table.
            for (HKeySegment parentHKeySegment : parentHKey.segments()) {
                HKeySegment segment = hKey.addSegment(parentHKeySegment.table());
                for (HKeyColumn parentHKeyColumn : parentHKeySegment.columns()) {
                    Column columnInChild = join.getMatchingChild(parentHKeyColumn.column());
                    Column segmentColumn = columnInChild == null ? parentHKeyColumn.column() : columnInChild;
                    segment.addColumn(segmentColumn);
                    hKeyColumns.add(segmentColumn);
                }
            }
        }
        // This table's hkey also includes any PK columns not already included.
        HKeySegment newSegment = hKey.addSegment(this);
        for (Column pkColumn : getPrimaryKeyIncludingInternal().getColumns()) {
            if (!hKeyColumns.contains(pkColumn)) {
                newSegment.addColumn(pkColumn);
            }
        }
        // Determine whether the table contains its own hkey, i.e., whether all hkey columns come from this table.
        containsOwnHKey = true;
        for (HKeySegment segment : hKey().segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                if (hKeyColumn.column().getTable() != this) {
                    containsOwnHKey = false;
                }
            }
        }
    }

    private TableIndex createAkibanPrimaryKeyIndex()
    {
        // Create a column for a PK
        Column pkColumn = Column.create(this,
                                        Column.AKIBAN_PK_NAME,
                                        getColumns().size(),
                                        Types.BIGINT); // adds column to table
        pkColumn.setNullable(false);
        // Create an index for the PK column
        // Starting index should be id 1
        int maxIndexId = 0;
        for (Index index : getIndexes()) {
            if (index.getIndexId() > maxIndexId) {
                maxIndexId = index.getIndexId();
            }
        }
        TableIndex pkIndex = TableIndex.create(ais,
                                               this,
                                               Index.PRIMARY_KEY_CONSTRAINT,
                                               maxIndexId + 1,
                                               true,
                                               Index.PRIMARY_KEY_CONSTRAINT);
        IndexColumn pkIndexColumn = new IndexColumn(pkIndex, pkColumn, 0, true, null);
        pkIndex.addColumn(pkIndexColumn);
        return pkIndex;
    }

    private static Collection<TableIndex> removeInternalColumnIndexes(Collection<TableIndex> indexes)
    {
        Collection<TableIndex> declaredIndexes = new ArrayList<TableIndex>(indexes);
        for (Iterator<TableIndex> iterator = declaredIndexes.iterator(); iterator.hasNext();) {
            TableIndex index = iterator.next();
            List<IndexColumn> indexColumns = index.getKeyColumns();
            if (indexColumns.size() == 1 && indexColumns.get(0).getColumn().isAkibanPKColumn()) {
                iterator.remove();
            }
        }
        return declaredIndexes;
    }

    // State

    private int size;
    private List<Join> candidateParentJoins = new ArrayList<Join>();
    private List<Join> candidateChildJoins = new ArrayList<Join>();
    private PrimaryKey primaryKey;
    private transient HKey hKey;
    private transient boolean containsOwnHKey;
    private transient HKey branchHKey;
    private transient List<Column> allHKeyColumns;
    private transient Integer depth = null;
    private final Object lazyEvaluationLock = new Object();
    private transient volatile List<UserTable> hKeyDependentTables;

    // consts

    private static final Comparator<Column> COLUMNS_BY_TABLE_DEPTH = new Comparator<Column>() {
        @Override
        public int compare(Column o1, Column o2) {
            return o1.getUserTable().getDepth() - o2.getUserTable().getDepth();
        }
    };
}
