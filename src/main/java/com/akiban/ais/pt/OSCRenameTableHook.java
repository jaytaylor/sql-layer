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

package com.akiban.ais.pt;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.*;
import com.akiban.ais.util.TableChange;
import com.akiban.message.MessageRequiredServices;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.InvalidAlterException;
import com.akiban.server.service.session.Session;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.util.*;

/** Hook for <code>RenameTableRequest</code>.
 * The single statement <code>RENAME TABLE xxx TO _xxx_old, _xxx_new TO xxx</code>
 * arrives from the adapter as two requests. For simplicity, and since
 * rename is cheap, the first is allowed to proceed, but the target name is noted.
 * The second is where all the work this has been leading up to happens: the
 * alter is performed on <code>xxx</code> and new is renamed to old.
 * In this way, at the end of the atomic rename, the client sees the tables that
 * it expects with the shape that it expects. But not arrived at in
 * the way it orchestrated.
 */
public class OSCRenameTableHook
{
    private final MessageRequiredServices requiredServices;

    public OSCRenameTableHook(MessageRequiredServices requiredServices) {
        this.requiredServices = requiredServices;
    }

    public boolean before(Session session, TableName oldName, TableName newName) {
        if ((newName.getTableName().charAt(0) == '_') &&
            newName.getTableName().endsWith("_old") &&
            newName.getSchemaName().equals(oldName.getSchemaName())) {
            return beforeOld(session, oldName, newName);
        }
        else if ((oldName.getTableName().charAt(0) == '_') &&
                 oldName.getTableName().endsWith("_new") &
                 oldName.getSchemaName().equals(newName.getSchemaName())) {
            return beforeNew(session, oldName, newName);
        }
        return true;            // Allow rename to continue.
    }

    /** Handle first rename.
     * We are being told to rename <code>xxx</code> to <code>_xxx_old</code>.
     * If this is OSC, there is an <code>_xxx_new</code> that points to <code>xxx</code>.
     * Except that either one of those _ names might have multiple _'s for uniqueness.
     */
    protected boolean beforeOld(Session session, TableName oldName, TableName newName) {
        AkibanInformationSchema ais = ais(session);
        String schemaName = oldName.getSchemaName();
        String tableName = oldName.getTableName();

        // Easy case first.
        UserTable table = ais.getUserTable(schemaName, "_" + tableName + "_new");
        if ((table != null) &&
            (table.getPendingOSC() != null) &&
            (table.getPendingOSC().getOriginalName().equals(tableName))) {
            table.getPendingOSC().setCurrentName(newName.getTableName());
            return true;
        }
        
        // Try harder.
        for (Map.Entry<String,UserTable> entry : ais.getSchema(schemaName).getUserTables().entrySet()) {
            if (entry.getKey().contains(tableName) &&
                (table.getPendingOSC() != null) &&
                (table.getPendingOSC().getOriginalName().equals(tableName))) {
                table.getPendingOSC().setCurrentName(newName.getTableName());
                return true;
            }
        }

        return true;
    }

    /** Handle second rename.
     * Undo the first rename. So far that is the only change to real data that has
     * been made. If we fail now because of grouping constraints, we are in as good
     * shape as is possible under those circumstances.
     * Then do the alter on the original table.
     * Then rename the temp table to the name that OSC will DROP.
     */
    protected boolean beforeNew(Session session, TableName oldName, TableName newName) {
        AkibanInformationSchema ais = ais(session);
        UserTable tempTable = ais.getUserTable(oldName);
        if (tempTable == null) return true;
        PendingOSC osc = tempTable.getPendingOSC();
        if ((osc == null) ||
            (osc.getCurrentName() == null))
            return true;
        TableName currentName = new TableName(oldName.getSchemaName(), osc.getCurrentName());
        if (ais.getUserTable(currentName) == null)
            return true;
        
        TableName origName = new TableName(oldName.getSchemaName(), osc.getOriginalName());
        ddl().renameTable(session, currentName, origName);
        
        doAlter(session, origName, oldName, osc);

        ddl().renameTable(session, oldName, currentName);
        return false;
    }

    /** Do the actual alter, corresponding to what was done previously
     * on a temporary copy of the table. Because we have this copy as
     * a template, not very much information needs to be remembered
     * about the earlier alter.
     */
    protected void doAlter(Session session, TableName origName, TableName tempName, PendingOSC changes) {
        AkibanInformationSchema ais = ais(session);
        UserTable origTable = ais.getUserTable(origName);
        UserTable tempTable = ais.getUserTable(tempName);
        Set<Column> droppedColumns = new HashSet<Column>();
        BiMap<Column,Column> modifiedColumns = HashBiMap.<Column,Column>create();
        Set<Column> addedColumns = new HashSet<Column>();
        getColumnChanges(origTable, tempTable, changes.getColumnChanges(), 
                         droppedColumns, modifiedColumns, addedColumns);
        Set<TableIndex> droppedIndexes = new HashSet<TableIndex>();
        BiMap<TableIndex,TableIndex> modifiedIndexes = HashBiMap.<TableIndex,TableIndex>create();
        Set<TableIndex> addedIndexes = new HashSet<TableIndex>();
        getIndexChanges(origTable, tempTable, changes.getIndexChanges(), 
                        droppedIndexes, modifiedIndexes, addedIndexes);
        AkibanInformationSchema aisCopy = AISCloner.clone(ais, new GroupSelector(origTable.getGroup()));
        UserTable copyTable = aisCopy.getUserTable(origName);
        rebuildColumns(origTable, copyTable,
                       droppedColumns, modifiedColumns, addedColumns);
        rebuildIndexes(origTable, copyTable,
                       droppedIndexes, modifiedIndexes, addedIndexes);
        rebuildGroup(aisCopy, copyTable);
        copyTable.endTable();
        ddl().alterTable(session, origName, copyTable, 
                         changes.getColumnChanges(), changes.getIndexChanges(), null);
    }

    private void getColumnChanges(UserTable origTable, UserTable tempTable, Collection<TableChange> changes,
                                  Set<Column> droppedColumns, Map<Column,Column> modifiedColumns, Set<Column> addedColumns) {
        for (TableChange  columnChange : changes) {
            switch (columnChange.getChangeType()) {
            case DROP:
                {
                    Column oldColumn = origTable.getColumn(columnChange.getOldName());
                    if (oldColumn == null)
                        throw new InvalidAlterException(origTable.getName(), "Could not find dropped column " + columnChange);
                    droppedColumns.add(oldColumn);
                }
                break;
            case MODIFY:
                {
                    Column oldColumn = origTable.getColumn(columnChange.getOldName());
                    Column newColumn = tempTable.getColumn(columnChange.getNewName());
                    if ((oldColumn == null) || (newColumn == null))
                        throw new InvalidAlterException(origTable.getName(), "Could not find modified column " + columnChange);
                    modifiedColumns.put(oldColumn, newColumn);
                }
                break;
            case ADD:
                {
                    Column newColumn = tempTable.getColumn(columnChange.getNewName());
                    if (newColumn == null)
                        throw new InvalidAlterException(origTable.getName(), "Could not find added column " + columnChange);
                    addedColumns.add(newColumn);
                }
                break;
            }
        }
    }

    private void rebuildColumns(UserTable origTable, UserTable copyTable,
                                Set<Column> droppedColumns, Map<Column,Column> modifiedColumns, Set<Column> addedColumns) {
        copyTable.dropColumns();
        int colpos = 0;
        for (Column origColumn : origTable.getColumns()) {
            if (droppedColumns.contains(origColumn)) 
                continue;
            Column fromColumn = origColumn;
            Column newColumn = modifiedColumns.get(origColumn);
            if (newColumn != null) 
                fromColumn = newColumn;
            // Note that dropping columns can still cause otherwise
            // unchanged ones to move.
            Column.create(copyTable, fromColumn, null, colpos++);
        }
        for (Column newColumn : addedColumns) {
            Column.create(copyTable, newColumn, null, colpos++);
        }
    }

    private void getIndexChanges(UserTable origTable, UserTable tempTable, Collection<TableChange> changes,
                                 Set<TableIndex> droppedIndexes, Map<TableIndex,TableIndex> modifiedIndexes, Set<TableIndex> addedIndexes) {
        for (TableChange indexChange : changes) {
            switch (indexChange.getChangeType()) {
            case DROP:
                {
                    TableIndex oldIndex = origTable.getIndex(indexChange.getOldName());
                    if (oldIndex == null)
                        throw new InvalidAlterException(origTable.getName(), "Could not find dropped index " + indexChange);
                    droppedIndexes.add(oldIndex);
                }
                break;
            case MODIFY:
                {
                    TableIndex oldIndex = origTable.getIndex(indexChange.getOldName());
                    TableIndex newIndex = tempTable.getIndex(indexChange.getNewName());
                    if ((oldIndex == null) || (newIndex == null))
                        throw new InvalidAlterException(origTable.getName(), "Could not find modified index " + indexChange);
                    modifiedIndexes.put(oldIndex, newIndex);
                }
                break;
            case ADD:
                {
                    TableIndex newIndex = tempTable.getIndex(indexChange.getNewName());
                    if (newIndex == null)
                        throw new InvalidAlterException(origTable.getName(), "Could not find added index " + indexChange);
                    addedIndexes.add(newIndex);
                }
                break;
            }
        }
    }

    private void rebuildIndexes(UserTable origTable, UserTable copyTable,
                                Set<TableIndex> droppedIndexes, Map<TableIndex,TableIndex> modifiedIndexes, Set<TableIndex> addedIndexes) {
        copyTable.removeIndexes(copyTable.getIndexesIncludingInternal());
        for (TableIndex origIndex : origTable.getIndexes()) {
            if (droppedIndexes.contains(origIndex)) 
                continue;
            TableIndex fromIndex = origIndex;
            TableIndex newIndex = modifiedIndexes.get(origIndex);
            if (newIndex != null) 
                fromIndex = newIndex;
            rebuildIndex(origTable, copyTable, fromIndex);
        }
        for (TableIndex newIndex : addedIndexes) {
            rebuildIndex(origTable, copyTable, newIndex);
        }
    }

    private void rebuildIndex(UserTable origTable, UserTable copyTable, TableIndex fromIndex) {
        TableIndex copyIndex = TableIndex.create(copyTable, fromIndex);
        int idxpos = 0;
        for (IndexColumn indexColumn : fromIndex.getKeyColumns()) {
            Column copyColumn = copyTable.getColumn(indexColumn.getColumn().getName());
            if (copyColumn == null)
                // This assumes that index implicitly dropped by
                // removing all its columns was included in
                // indexChanges passed from adapter.
                throw new InvalidAlterException(origTable.getName(), "Could not find index column " + indexColumn);
            IndexColumn.create(copyIndex, copyColumn, indexColumn, idxpos++);
        }
    }

    // This assumes that OSC was not used to deliberately affect the group, by changing
    // grouping constraints, say, since the group structure is not on the master.
    // So it only deals with unintended consequences.
    private void rebuildGroup(AkibanInformationSchema ais, UserTable table) {
        rebuildGroupIndexes(ais, table);
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            table.removeCandidateParentJoin(parentJoin);
            if (joinStillValid(parentJoin, table, false)) {
                parentJoin = rebuildJoin(ais, parentJoin, table, false);
                table.addCandidateParentJoin(parentJoin);
            }
        }
        for (Join childJoin : table.getChildJoins()) {
            table.removeCandidateChildJoin(parentJoin);
            if (joinStillValid(childJoin, table, true)) {
                childJoin = rebuildJoin(ais, childJoin, table, true);
                table.addCandidateChildJoin(childJoin);
            }
        }
    }

    private void rebuildGroupIndexes(AkibanInformationSchema ais, UserTable table) {
        Group group = table.getGroup();
        List<GroupIndex> dropIndexes = new ArrayList<GroupIndex>();
        List<GroupIndex> copyIndexes = new ArrayList<GroupIndex>();
        for (GroupIndex groupIndex : group.getIndexes()) {
            boolean found = false, drop = false;
            for (IndexColumn indexColumn : groupIndex.getKeyColumns()) {
                if (indexColumn.getColumn().getTable() == table) {
                    found = true;
                    if (table.getColumn(indexColumn.getColumn().getName()) == null) {
                        drop = true;
                    }
                }
            }
            if (found) {
                if (drop)
                    dropIndexes.add(groupIndex);
                else
                    copyIndexes.add(groupIndex);
            }
        }
        group.removeIndexes(dropIndexes);
        group.removeIndexes(copyIndexes);
        for (GroupIndex oldIndex : copyIndexes) {
            GroupIndex newIndex = GroupIndex.create(ais, group, oldIndex);
            int idxpos = 0;
            for (IndexColumn indexColumn : oldIndex.getKeyColumns()) {
                Column column = indexColumn.getColumn();
                if (column.getTable() == table)
                    column = table.getColumn(column.getName());
                IndexColumn.create(newIndex, column, indexColumn, idxpos++);
            }
        }
    }

    private boolean joinStillValid(Join join, UserTable table, boolean asParent) {
        for (JoinColumn joinColumn : join.getJoinColumns()) {
            Column column = (asParent) ? joinColumn.getParent() : joinColumn.getChild();
            assert (column.getTable() == table);
            if (table.getColumn(column.getName()) == null)
                return false;
        }
        return true;
    }
    
    private Join rebuildJoin(AkibanInformationSchema ais, Join oldJoin, UserTable table, boolean asParent) {
        Join newJoin = Join.create(ais, oldJoin.getName(), oldJoin.getParent(), oldJoin.getChild());
        for (JoinColumn joinColumn : oldJoin.getJoinColumns()) {
            Column parent = joinColumn.getParent();
            Column child = joinColumn.getChild();
            if (asParent)
                parent = table.getColumn(parent.getName());
            else
                child = table.getColumn(child.getName());
            newJoin.addJoinColumn(parent, child);
        }
        return newJoin;
    }

    private static class GroupSelector extends com.akiban.ais.protobuf.ProtobufWriter.TableSelector {
        private final Group group;

        public GroupSelector(Group group) {
            this.group = group;
        }

        @Override
        public boolean isSelected(Columnar columnar) {
            return columnar.isTable() && ((Table)columnar).getGroup() == group;
        }
    }

    private AkibanInformationSchema ais(Session session) {
        return requiredServices.schemaManager().getAis(session);
    }

    private DDLFunctions ddl() {
        return requiredServices.dxl().ddlFunctions();
    }
}
