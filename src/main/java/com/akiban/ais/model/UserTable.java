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

import com.akiban.qp.memoryadapter.MemoryTableFactory;
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

    public void clearGrouping() {
        candidateChildJoins.clear();
        candidateParentJoins.clear();
        group = null;
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
            allHKeyColumns = Collections.unmodifiableList(allHKeyColumns);
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

    public boolean hasMemoryTableFactory()
    {
        return tableFactory != null;
    }

    public MemoryTableFactory getMemoryTableFactory()
    {
        return tableFactory;
    }

    public void setMemoryTableFactory(MemoryTableFactory tableFactory)
    {
        this.tableFactory = tableFactory;
    }

    public boolean hasVersion()
    {
        return version != null;
    }

    public Integer getVersion()
    {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
    
    private void addTableAndDescendents(UserTable table, List<UserTable> accumulator)
    {
        accumulator.add(table);
        for (Join join : table.getChildJoins()) {
            addTableAndDescendents(join.getChild(), accumulator);
        }
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
        IndexColumn.create(pkIndex, pkColumn, 0, true, null);
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

    private final List<Join> candidateParentJoins = new ArrayList<Join>();
    private final List<Join> candidateChildJoins = new ArrayList<Join>();
    private final Object lazyEvaluationLock = new Object();

    private PrimaryKey primaryKey;
    private HKey hKey;
    private boolean containsOwnHKey;
    private HKey branchHKey;
    private List<Column> allHKeyColumns;
    private Integer depth = null;
    private volatile List<UserTable> hKeyDependentTables;
    private volatile List<UserTable> ancestors;
    private MemoryTableFactory tableFactory;
    private Integer version;

    // consts

    private static final Comparator<Column> COLUMNS_BY_TABLE_DEPTH = new Comparator<Column>() {
        @Override
        public int compare(Column o1, Column o2) {
            return o1.getUserTable().getDepth() - o2.getUserTable().getDepth();
        }
    };
}
