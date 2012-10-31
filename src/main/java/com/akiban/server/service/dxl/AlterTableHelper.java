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

package com.akiban.server.service.dxl;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.TableChange;
import com.akiban.server.service.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.akiban.ais.util.TableChangeValidator.TableColumnNames;

public class AlterTableHelper {
    final List<TableChange> columnChanges;
    final List<TableChange> indexChanges;
    final Map<IndexName, List<TableColumnNames>> affectedGroupIndexes;

    public AlterTableHelper(List<TableChange> columnChanges, List<TableChange> indexChanges,
                            Map<IndexName, List<TableColumnNames>> affectedGroupIndexes) {
        checkChangeTypes(columnChanges);
        checkChangeTypes(indexChanges);
        this.columnChanges = columnChanges;
        this.indexChanges = indexChanges;
        this.affectedGroupIndexes = affectedGroupIndexes;
    }

    private static void checkChangeTypes(List<TableChange> changes) {
        for(TableChange change : changes) {
            switch(change.getChangeType()) {
                case ADD:
                case DROP:
                case MODIFY:
                break;
                default:
                    throw new IllegalStateException("Unknown change type: " + change);
            }
        }
    }

    public Integer findOldPosition(UserTable oldTable, Column newColumn) {
        String newName = newColumn.getName();
        for(TableChange change : columnChanges) {
            if(newName.equals(change.getNewName())) {
                switch(change.getChangeType()) {
                    case ADD:
                        return null;
                    case MODIFY:
                        Column oldColumn = oldTable.getColumn(change.getOldName());
                        assert oldColumn != null : newColumn;
                        return oldColumn.getPosition();
                    case DROP:
                        throw new IllegalStateException("Dropped new column? " + newName);
                }
            }
        }
        Column oldColumn = oldTable.getColumn(newName);
        if((oldColumn == null) && newColumn.isAkibanPKColumn()) {
            return null;
        }
        // Not in change list, must be an original column
        assert oldColumn != null : newColumn;
        return oldColumn.getPosition();
    }

    public List<Index> findNewIndexesToBuild(UserTable newTable) {
        List<Index> indexes = new ArrayList<Index>();
        for(TableChange change : indexChanges) {
            switch(change.getChangeType()) {
                case ADD:
                case MODIFY:
                    indexes.add(newTable.getIndexIncludingInternal(change.getNewName()));
                break;
            }
        }
        return indexes;
    }

    public void dropAffectedGroupIndexes(Session session, BasicDDLFunctions ddl, UserTable origTable) {
        // Drop definition and rebuild later, probably better than doing each entry individually
        if(affectedGroupIndexes.isEmpty()) {
            return;
        }
        List<GroupIndex> groupIndexes = new ArrayList<GroupIndex>();
        for(IndexName name : affectedGroupIndexes.keySet()) {
            groupIndexes.add(origTable.getGroup().getIndex(name.getName()));
        }
        ddl.schemaManager().dropIndexes(session, groupIndexes);
    }

    public void createAffectedGroupIndexes(Session session, BasicDDLFunctions ddl, UserTable origTable, UserTable newTable, boolean dataChange) {
        // Ideally only would copy the Group, but that is vulnerable to changing group names. Even if we handle that
        // by looking up the new name, index creation in PSSM requires index.getName().getTableName() match the actual.
        AkibanInformationSchema tempAIS = AISCloner.clone(newTable.getAIS());

        List<Index> indexesToBuild = new ArrayList<Index>();
        Group origGroup = origTable.getGroup();
        Group tempGroup = tempAIS.getGroup(newTable.getGroup().getName());
        for(Map.Entry<IndexName, List<TableColumnNames>> entry : affectedGroupIndexes.entrySet()) {
            GroupIndex origIndex = origGroup.getIndex(entry.getKey().getName());
            List<TableColumnNames> columns = entry.getValue();
            // TableChangeValidator returns the index with no remaining columns
            if(columns.isEmpty()) {
                continue;
            }
            GroupIndex tempIndex = GroupIndex.create(tempAIS, tempGroup, origIndex);
            for(int i = 0; i < columns.size(); ++i) {
                TableColumnNames tcn = columns.get(i);
                UserTable tempTable = tempAIS.getUserTable(tcn.tableName);
                Column tempColumn = tempTable.getColumn(tcn.newColumnName);
                IndexColumn.create(tempIndex,  tempColumn, i, true, null);
            }
            indexesToBuild.add(tempIndex);
        }

        if(dataChange) {
            ddl.createIndexesInternal(session, indexesToBuild);
        } else {
            ddl.schemaManager().createIndexes(session, indexesToBuild);
            // Restore old trees
            Group newGroup = ddl.getAIS(session).getTable(newTable.getName()).getGroup();
            for(IndexName name : affectedGroupIndexes.keySet()) {
                Index oldIndex = origGroup.getIndex(name.getName());
                Index newIndex = newGroup.getIndex(name.getName());
                newIndex.setTreeName(oldIndex.getTreeName());
            }
        }
    }
}
