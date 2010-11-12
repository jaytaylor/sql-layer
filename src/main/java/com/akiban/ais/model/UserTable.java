/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class UserTable extends Table
{
    public static UserTable create(AkibaInformationSchema ais,
                                   String schemaName,
                                   String tableName,
                                   Integer tableId)
    {
        UserTable userTable = new UserTable(ais, schemaName, tableName, tableId);
        ais.addUserTable(userTable);
        return userTable;
    }

    public UserTable(AkibaInformationSchema ais, String schemaName, String tableName, Integer tableId)
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
    public String toString()
    {
        return
            getGroup() == null
            ? "UserTable(" + tableName + ", group(null))"
            : "UserTable(" + tableName + ", group(" + getGroup().getName() + "))";
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
    public void traversePreOrder(Visitor visitor) throws Exception
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
    public void traversePostOrder(Visitor visitor) throws Exception
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

        if (primaryKey == null) {
            // Find primary key index
            Index primaryKeyIndex = null;
            for (Index index : getIndexes()) {
                if (index.isPrimaryKey()) {
                    primaryKeyIndex = index;
                }
            }
            if (primaryKeyIndex != null) {
                primaryKey = new PrimaryKey(primaryKeyIndex);
            }
        }
        return primaryKey;
    }

    public Integer getDepth()
    {
        return
            getGroup() == null ? null :
            getParentJoin() == null ? 0 : getParentJoin().getParent().getDepth() + 1;
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

    public List<Column> allHKeyColumns()
    {
        assert getGroup() != null;
        assert getPrimaryKey() != null;
        if (allHKeyColumns == null) {
            computeHKeyColumns();
        }
        return allHKeyColumns;
    }

    public List<Column> localHKeyColumns()
    {
        assert getGroup() != null;
        assert getPrimaryKey() != null;
        if (localHKeyColumns == null) {
            computeHKeyColumns();
        }
        return localHKeyColumns;
    }

    public boolean containsOwnHKey()
    {
        for (Column column : allHKeyColumns()) {
            if (column.getTable() != this) {
                return false;
            }
        }
        assert localHKeyColumns().equals(allHKeyColumns());
        return true;
    }

    @SuppressWarnings("unused")
    private UserTable()
    {
        // XXX: GWT requires empty constructor
    }

    private void computeHKeyColumns()
    {
        if (isRoot()) {
            List<Column> hkey = getPrimaryKey().getColumns();
            localHKeyColumns = Collections.unmodifiableList(hkey);
            allHKeyColumns = localHKeyColumns;
        } else {
            // Start with the parent's hkey
            Join join = getParentJoin();
            List<Column> parentHKey = parentTable().allHKeyColumns();
            // Start forming this table's full by including all of the parent hkey columns, but replacing
            // columns participating in the join (to this table) by columns from this table.
            List<Column> hkey = new ArrayList<Column>();
            for (Column parentHKeyColumn : parentHKey) {
                Column hKeyColumn = join.getMatchingChild(parentHKeyColumn);
                hkey.add(hKeyColumn == null ? parentHKeyColumn : hKeyColumn);
            }
            // This table's hkey also includes any PK columns not already included.
            for (Column pkColumn : getPrimaryKey().getColumns()) {
                if (!hkey.contains(pkColumn)) {
                    hkey.add(pkColumn);
                }
            }
            allHKeyColumns = Collections.unmodifiableList(new ArrayList<Column>(hkey));
            // Local hkey columns is allHKey columns without columns from other tables
            for (Iterator<Column> i = hkey.iterator(); i.hasNext();) {
                Column hKeyColumn = i.next();
                if (hKeyColumn.getUserTable() != this) {
                    i.remove();
                }
            }
            localHKeyColumns = Collections.unmodifiableList(new ArrayList<Column>(hkey));
        }
    }

    private UserTable parentTable()
    {
        UserTable parentTable = null;
        if (getParentJoin() != null) {
            parentTable = getParentJoin().getParent();
        }
        return parentTable;
    }

    // State

    private int size;
    private List<Join> candidateParentJoins = new ArrayList<Join>();
    private List<Join> candidateChildJoins = new ArrayList<Join>();
    private PrimaryKey primaryKey;
    private transient List<Column> localHKeyColumns;
    private transient List<Column> allHKeyColumns;
}
