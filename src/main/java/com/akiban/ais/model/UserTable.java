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
    
    public void setEngine(String engine){
        this.engine = engine;
    }

    @SuppressWarnings("unused")
    private UserTable()
    {
        // XXX: GWT requires empty constructor
    }

    // State

    private int size;
    private List<Join> candidateParentJoins = new ArrayList<Join>();
    private List<Join> candidateChildJoins = new ArrayList<Join>();
    private PrimaryKey primaryKey;
}
