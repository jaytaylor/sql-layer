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

import com.foundationdb.qp.rowtype.InternalIndexTypes;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.util.ArgumentValidation;

import java.util.*;

public class Table extends Columnar implements HasGroup, Visitable
{
    public static Table create(AkibanInformationSchema ais,
                               String schemaName,
                               String tableName,
                               Integer tableId)
    {
        Table table = new Table(ais, schemaName, tableName, tableId);
        ais.addTable(table);
        return table;
    }

    /**
     * Create an independent copy of an existing Table.
     * @param ais Destination AkibanInformationSchema.
     * @param table Table to copy.
     * @return The new copy of the Table.
     */
    public static Table create(AkibanInformationSchema ais, Table table) {
        Table copy = create(ais, table.tableName.getSchemaName(), table.tableName.getTableName(), table.getTableId());
        copy.setUuid(table.getUuid());
        return copy;
    }

    private Table(AkibanInformationSchema ais, String schemaName, String tableName, Integer tableId)
    {
        super(ais, schemaName, tableName);
        this.tableId = tableId;

        this.groupIndexes = new HashSet<>();
        this.unmodifiableGroupIndexes = Collections.unmodifiableCollection(groupIndexes);
        this.indexMap = new TreeMap<>();
        this.unmodifiableIndexMap = Collections.unmodifiableMap(indexMap);
        this.fullTextIndexes = new HashSet<>();
        this.unmodifiableFullTextIndexes = Collections.unmodifiableCollection(fullTextIndexes);
        this.foreignKeys = new LinkedHashSet<>();
        this.unmodifiableForeignKeys = Collections.unmodifiableCollection(foreignKeys);
    }

    @Override
    public boolean isView() {
        return false;
    }

    public Integer getTableId()
    {
        return tableId;
    }

    /**
     * Temporary mutator so that prototype AIS management can renumber all
     * the tables once created.  Longer term we want to give the table
     * its ID when generated.
     *
     * @param tableId
     */
    public void setTableId(final int tableId)
    {
        this.tableId = tableId;
    }

    public Integer getOrdinal()
    {
        return ordinal;
    }

    public void setOrdinal(Integer ordinal)
    {
        this.ordinal = ordinal;
    }

    public Group getGroup()
    {
        return group;
    }

    public void setGroup(Group group)
    {
        this.group = group;
    }

    public Collection<TableIndex> getIndexesIncludingInternal()
    {
        return unmodifiableIndexMap.values();
    }

    public Collection<TableIndex> getIndexes()
    {
        Collection<TableIndex> indexes = getIndexesIncludingInternal();
        return removeInternalColumnIndexes(indexes);
    }

    public TableIndex getIndexIncludingInternal(String indexName)
    {
        return unmodifiableIndexMap.get(indexName);
    }

    public TableIndex getIndex(String indexName)
    {
        TableIndex index = null;
        if (indexName.equals(Index.PRIMARY_KEY_CONSTRAINT)) {
            // getPrimaryKey has logic for handling hidden PK
            PrimaryKey primaryKey = getPrimaryKey();
            index = primaryKey == null ? null : primaryKey.getIndex();
        } else {
            index = getIndexIncludingInternal(indexName);
        }
        return index;
    }

    /**
     * Get all GroupIndexes this table participates in, both explicit and implicit (i.e. as a declared column or
     * ancestor of a participating table).
     */
    public final Collection<GroupIndex> getGroupIndexes() {
        return unmodifiableGroupIndexes;
    }

    protected void addIndex(TableIndex index)
    {
        indexMap.put(index.getIndexName().getName(), index);
        if (index.isPrimaryKey()) {
            assert primaryKey == null;
            primaryKey = new PrimaryKey(index);
        }
    }

    void clearIndexes() {
        indexMap.clear();
    }

    final void addGroupIndex(GroupIndex groupIndex) {
        groupIndexes.add(groupIndex);
    }

    final void removeGroupIndex(GroupIndex groupIndex) {
        groupIndexes.remove(groupIndex);
    }

    public void removeIndexes(Collection<TableIndex> indexesToDrop) {
        if((primaryKey != null) && indexesToDrop.contains(primaryKey.getIndex())) {
            primaryKey = null;
        }
        indexMap.values().removeAll(indexesToDrop);
    }

    public void rowDef(RowDef rowDef)
    {
        this.rowDef = rowDef;
    }

    public RowDef rowDef()
    {
        return rowDef;
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
        List<Column> matchingColumns = new ArrayList<>();
        matchingColumns.add(column);
        findMatchingAncestorColumns(column, matchingColumns);
        findMatchingDescendantColumns(column, matchingColumns);
        Collections.sort(matchingColumns, COLUMNS_BY_TABLE_DEPTH);
        return matchingColumns;
    }

    private void findMatchingAncestorColumns(Column fromColumn, List<Column> matchingColumns)
    {
        Join join = fromColumn.getTable().getParentJoin();
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
                join.getChild().findMatchingDescendantColumns(descendantJoinColumn.getChild(), matchingColumns);
            }
        }
    }

    public void addCandidateParentJoin(Join parentJoin)
    {
        candidateParentJoins.add(parentJoin);
    }

    public void addCandidateChildJoin(Join childJoin)
    {
        candidateChildJoins.add(childJoin);
    }

    public void removeCandidateParentJoin(Join parentJoin)
    {
        candidateParentJoins.remove(parentJoin);
    }

    public void removeCandidateChildJoin(Join childJoin)
    {
        candidateChildJoins.remove(childJoin);
    }

    public List<Join> getCandidateParentJoins()
    {
        return Collections.unmodifiableList(candidateParentJoins);
    }

    public List<Join> getCandidateChildJoins()
    {
        return Collections.unmodifiableList(candidateChildJoins);
    }

    public boolean hasChildren() {
        return !getCandidateChildJoins().isEmpty();
    }

    public Table getParentTable() {
        Join j = getParentJoin();
        return (j != null) ? j.getParent() : null;
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
        List<Join> childJoins = new ArrayList<>();
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
    
    public Column getIdentityColumn() 
    {
        Column identity = null;
        for (Column column : this.getColumnsIncludingInternal()) {
            if (column.getIdentityGenerator() != null) {
                identity = column;
            }
        }
        return identity;
    }

    public boolean isDescendantOf(Table other) {
        if (getGroup() == null || !getGroup().equals(other.getGroup())) {
            return false;
        }
        Table possibleDescendant = this;
        while (possibleDescendant != null) {
            if (possibleDescendant.equals(other)) {
                return true;
            }
            possibleDescendant = possibleDescendant.getParentTable();
        }
        return false;
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
            
            // TODO: This could be replace by a call to PrimaryKey#isAkibanPK()
            // But there is some dependecy here which causes the tests to fail if you do so.
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

    public synchronized void endTable(NameGenerator generator)
    {
        addHiddenPrimaryKey(generator);

        // Put the columns into our list
        TreeSet<String> entities = new TreeSet<String>();
        for (Column column : getColumns()) {
            entities.add(column.getName());
        }

        // put the child tables into their ordered list.
        TreeMap<String, Table> childTables = new TreeMap<>();
        for (Join childJoin : candidateChildJoins ) {
            String childName;
            if (childJoin.getChild().getName().getSchemaName().equals(getName().getSchemaName())) {
                childName = childJoin.getChild().getName().getTableName();
            } else {
                childName = childJoin.getChild().getName().toString();
            }
            childTables.put(childName, childJoin.getChild());
        }
       
        // Mangle the child table names to be unique with the "_"
        for (String child : childTables.keySet()) {
            String tryName = child;
            while (entities.contains(tryName)) {
                tryName = "_" + tryName;
            }
            childTables.get(child).nameForOutput = tryName;
            entities.add(tryName);
        }
        
        if (nameForOutput == null) {
            Join parentJoin = getParentJoin();
            if ((parentJoin != null) &&
                    parentJoin.getParent().getName().getSchemaName().equals(getName().getSchemaName())) {
                nameForOutput = getName().getTableName();
            } else {
                nameForOutput = getName().toString(); 
            }
        }

        for (ForeignKey fkey : foreignKeys) {
            fkey.findIndexes();
        }
    }

    public void addHiddenPrimaryKey(NameGenerator generator) {
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
                final int rootID;
                if(group == null) {
                    rootID = getTableId();
                } else {
                    assert group.getRoot() != null : "Null root: " + group;
                    rootID = group.getRoot().getTableId();
                }
                primaryKeyIndex = createAkibanPrimaryKeyIndex(generator.generateIndexID(rootID));
            }
            assert primaryKeyIndex != null : this;
            primaryKey = new PrimaryKey(primaryKeyIndex);
        }
    }

    public Integer getDepth()
    {
        if (depth == null && getGroup() != null) {
            synchronized (this) {
                if (depth == null && getGroup() != null) {
                    depth = getParentJoin() == null ? 0 : getParentJoin().getParent().getDepth() + 1;
                }
            }
        }
        return depth;
    }

    public Boolean isRoot()
    {
        return getGroup() == null || getParentJoin() == null;
    }

    public HKey hKey()
    {
        assert getGroup() != null;
        if (hKey == null) {
            computeHKey();
        }
        return hKey;
    }

    public List<Column> allHKeyColumns()
    {
        assert getGroup() != null;
        assert getPrimaryKeyIncludingInternal() != null;
        if (allHKeyColumns == null) {
            allHKeyColumns = new ArrayList<>();
            for (HKeySegment segment : hKey().segments()) {
                for (HKeyColumn hKeyColumn : segment.columns()) {
                    allHKeyColumns.add(hKeyColumn.column());
                }
            }
            allHKeyColumns = Collections.unmodifiableList(allHKeyColumns);
        }
        return allHKeyColumns;
    }

    public boolean containsOwnHKey()
    {
        hKey(); // Ensure hKey and containsOwnHKey are computed
        return containsOwnHKey;
    }

    public UUID getUuid() {
        return uuid;
    }

    public void setUuid(UUID uuid) {
        this.uuid = uuid;
    }

    // Descendent tables whose hkeys are affected by a change to this table's PK or FK.
    public List<Table> hKeyDependentTables()
    {
        if (hKeyDependentTables == null) {
            synchronized (lazyEvaluationLock) {
                if (hKeyDependentTables == null) {
                    hKeyDependentTables = new ArrayList<>();
                    for (Join join : getChildJoins()) {
                        Table child = join.getChild();
                        if (!child.containsOwnHKey()) {
                            addTableAndDescendents(child, hKeyDependentTables);
                        }
                    }
                }
            }
        }
        return hKeyDependentTables;
    }

    public boolean hasMemoryTableFactory()
    {
        return (group != null) && group.hasMemoryTableFactory();
    }

    public boolean hasVersion()
    {
        return version != null;
    }

    public Integer getVersion()
    {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public String getNameForOutput() {
        return nameForOutput;
    }
    
    private void addTableAndDescendents(Table table, List<Table> accumulator)
    {
        accumulator.add(table);
        for (Join join : table.getChildJoins()) {
            addTableAndDescendents(join.getChild(), accumulator);
        }
    }
    
    private void computeHKey()
    {
        hKey = new HKey(this);
        List<Column> hKeyColumns = new ArrayList<>();
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

    private TableIndex createAkibanPrimaryKeyIndex(int indexID)
    {
        // Create a column for a PK
        Column pkColumn = Column.create(this,
                                        Column.AKIBAN_PK_NAME,
                                        getColumns().size(),
                                        InternalIndexTypes.LONG.instance(false)); // adds column to table
        if (!this.hasMemoryTableFactory()) {
            // Create a sequence for the PK
            String schemaName = this.getName().getSchemaName();
            // Generates same (temporary) sequence name as AISBuilder, 
            // To catch (and reject) adding two sequences to the same table. 
            String sequenceName = this.getName().getTableName() + "-temp-sequence-1";
            Sequence identityGenerator = Sequence.create(ais, schemaName, sequenceName, 1L, 1L, 0L, Long.MAX_VALUE, false);
            // Set column as PK using sequence
            pkColumn.setDefaultIdentity(false);
            pkColumn.setIdentityGenerator(identityGenerator);
        }        
        // Create Primary key
        NameGenerator nameGenerator = new DefaultNameGenerator(ais);
        TableName constraintName = nameGenerator.generatePKConstraintName(this.getName().getSchemaName(), this.getName().getTableName());
        TableIndex pkIndex = TableIndex.create(ais,
                                               this,
                                               Index.PRIMARY_KEY_CONSTRAINT,
                                               indexID,
                                               true,
                                               Index.PRIMARY_KEY_CONSTRAINT,
                                               constraintName);
        IndexColumn.create(pkIndex, pkColumn, 0, true, null);
        return pkIndex;
    }

    private static Collection<TableIndex> removeInternalColumnIndexes(Collection<TableIndex> indexes)
    {
        Collection<TableIndex> declaredIndexes = new ArrayList<>(indexes);
        for (Iterator<TableIndex> iterator = declaredIndexes.iterator(); iterator.hasNext();) {
            TableIndex index = iterator.next();
            List<IndexColumn> indexColumns = index.getKeyColumns();
            if (indexColumns.size() == 1 && indexColumns.get(0).getColumn().isAkibanPKColumn()) {
                iterator.remove();
            }
        }
        return declaredIndexes;
    }

    public PendingOSC getPendingOSC() {
        return pendingOSC;
    }

    public void setPendingOSC(PendingOSC pendingOSC) {
        this.pendingOSC = pendingOSC;
    }

    /** Return all full text indexes in which this table participates. */
    public Collection<FullTextIndex> getFullTextIndexes() {
        return unmodifiableFullTextIndexes;
    }

    /** Return full text indexes that index this table. */
    public Collection<FullTextIndex> getOwnFullTextIndexes() {
        if (fullTextIndexes.isEmpty()) return Collections.emptyList();
        Collection<FullTextIndex> result = new ArrayList<>();
        for (FullTextIndex index : fullTextIndexes) {
            if (index.getIndexedTable() == this) {
                result.add(index);
            }
        }
        return result;
    }

    public void addFullTextIndex(FullTextIndex index) {
        fullTextIndexes.add(index);
    }

    /** Get a full text index this table participates in. */
    public FullTextIndex getFullTextIndex(String indexName) {
        for (FullTextIndex index : fullTextIndexes) {
            if (index.getIndexName().getName().equals(indexName)) {
                return index;
            }
        }
        return null;
    }

    public Collection<ForeignKey> getForeignKeys() {
        return unmodifiableForeignKeys;
    }

    public Collection<ForeignKey> getReferencingForeignKeys() {
        if (foreignKeys.isEmpty()) return Collections.emptyList();
        Collection<ForeignKey> result = new ArrayList<>();
        for (ForeignKey fk : foreignKeys) {
            if (fk.getReferencingTable() == this) {
                result.add(fk);
            }
        }
        return result;
    }

    public ForeignKey getReferencingForeignKey(String name) {
        for (ForeignKey fk : foreignKeys) {
            if ((fk.getReferencingTable() == this) &&
                fk.getConstraintName().getTableName().equals(name)) {
                return fk;
            }
        }
        return null;
    }

    public Collection<ForeignKey> getReferencedForeignKeys() {
        if (foreignKeys.isEmpty()) return Collections.emptyList();
        Collection<ForeignKey> result = new ArrayList<>();
        for (ForeignKey fk : foreignKeys) {
            if (fk.getReferencedTable() == this) {
                result.add(fk);
            }
        }
        return result;
    }

    public void addForeignKey(ForeignKey foreignKey) {
        ais.checkMutability();
        foreignKeys.add(foreignKey);
    }

    public void removeForeignKey(ForeignKey foreignKey) {
        ais.checkMutability();
        foreignKeys.remove(foreignKey);
    }

    // Visitable

    /** Visit this instance, every column, table index, full text index and then all children in depth first order. */
    @Override
    public void visit(Visitor visitor) {
        visit(visitor, true);
    }

    /** As {@link #visit(Visitor)} but visit children a snapshot of children in breadth first order. */
    public void visitBreadthFirst(Visitor visitor) {
        List<Table> remainingTables = new ArrayList<>();
        List<Join> remainingJoins = new ArrayList<>();
        remainingTables.add(this);
        remainingJoins.addAll(getCandidateChildJoins());
        // Add before visit in-case visitor changes group or joins
        while(!remainingJoins.isEmpty()) {
            Join join = remainingJoins.remove(remainingJoins.size() - 1);
            Table child = join.getChild();
            remainingTables.add(child);
            remainingJoins.addAll(child.getCandidateChildJoins());
        }
        for(Table table : remainingTables) {
            table.visit(visitor, false);
        }
    }

    private void visit(Visitor visitor, boolean recurse) {
        visitor.visit(this);
        for(Column c : getColumnsIncludingInternal()) {
            c.visit(visitor);
        }
        for(Index i : getIndexesIncludingInternal()) {
            i.visit(visitor);
        }
        for(Index i : getOwnFullTextIndexes()) {
            i.visit(visitor);
        }
        if(recurse) {
            for(Join t : getChildJoins()) {
                t.getChild().visit(visitor, recurse);
            }
        }
    }

    // State
    private final Map<String, TableIndex> indexMap;
    private final Map<String, TableIndex> unmodifiableIndexMap;
    private final Collection<GroupIndex> groupIndexes;
    private final Collection<GroupIndex> unmodifiableGroupIndexes;
    private final List<Join> candidateParentJoins = new ArrayList<>();
    private final List<Join> candidateChildJoins = new ArrayList<>();
    private final Object lazyEvaluationLock = new Object();

    private Group group;
    private Integer tableId;
    private RowDef rowDef;
    private Integer ordinal;
    private UUID uuid;
    private PrimaryKey primaryKey;
    private HKey hKey;
    private boolean containsOwnHKey;
    private List<Column> allHKeyColumns;
    private Integer depth = null;
    private volatile List<Table> hKeyDependentTables;
    private Integer version;
    private PendingOSC pendingOSC;
    private final Collection<FullTextIndex> fullTextIndexes;
    private final Collection<FullTextIndex> unmodifiableFullTextIndexes;
    private String nameForOutput;
    private final Collection<ForeignKey> foreignKeys;
    private final Collection<ForeignKey> unmodifiableForeignKeys;
    
    // consts

    private static final Comparator<Column> COLUMNS_BY_TABLE_DEPTH = new Comparator<Column>() {
        @Override
        public int compare(Column o1, Column o2) {
            return o1.getTable().getDepth() - o2.getTable().getDepth();
        }
    };
}
