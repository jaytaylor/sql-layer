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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger logger = LoggerFactory.getLogger(OSCRenameTableHook.class);

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
                (entry.getValue().getPendingOSC() != null) &&
                (entry.getValue().getPendingOSC().getOriginalName().equals(tableName))) {
                entry.getValue().getPendingOSC().setCurrentName(newName.getTableName());
                return true;
            }
        }
        // Still allow the rename to happen. Not having set the current name will
        // change how beforeNew works below.
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
        BiMap<Column,Column> modifiedColumns = HashBiMap.create();
        Set<Column> addedColumns = new HashSet<Column>();
        getColumnChanges(origTable, tempTable, changes.getColumnChanges(), 
                         droppedColumns, modifiedColumns, addedColumns);
        Set<TableIndex> droppedIndexes = new HashSet<TableIndex>();
        BiMap<TableIndex,TableIndex> modifiedIndexes = HashBiMap.create();
        Set<TableIndex> addedIndexes = new HashSet<TableIndex>();
        getIndexChanges(origTable, tempTable, changes.getIndexChanges(), 
                        droppedIndexes, modifiedIndexes, addedIndexes);
        AkibanInformationSchema aisCopy = AISCloner.clone(ais, new GroupSelector(origTable.getGroup()));
        UserTable copyTable = aisCopy.getUserTable(origName);
        rebuildColumns(origTable, tempTable, copyTable,
                       droppedColumns, modifiedColumns, addedColumns);
        rebuildIndexes(origTable, copyTable,
                       droppedIndexes, modifiedIndexes, addedIndexes);
        rebuildGroup(aisCopy, copyTable, changes.getColumnChanges());
        copyTable.endTable();
        logger.info("Pending OSC ALTER TABLE {} being done now", origName);
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

    private void rebuildColumns(UserTable origTable, UserTable tempTable, UserTable copyTable,
                                Set<Column> droppedColumns, BiMap<Column,Column> modifiedColumns, Set<Column> addedColumns) {
        copyTable.dropColumns();
        int colpos = 0;         // Respect column order of copy.
        for (Column tempColumn : tempTable.getColumns()) {
            Column fromColumn = tempColumn;
            // If neither added nor modified, try to find the original column.
            if (!addedColumns.contains(tempColumn) &&
                !modifiedColumns.containsValue(tempColumn)) {
                Column origColumn = origTable.getColumn(tempColumn.getName());
                if (origColumn != null)
                    fromColumn = origColumn;
            }
            Column.create(copyTable, fromColumn, null, colpos++);
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
                // An index implicitly dropped / modified by removing
                // its columns is not included in indexChanges passed
                // from adapter. Silently shorten it.
                continue;
            IndexColumn.create(copyIndex, copyColumn, indexColumn, idxpos++);
        }
        if (idxpos == 0)
            // Nothing was left. Remove index.
            copyTable.removeIndexes(Collections.singletonList(copyIndex));
    }

    // This assumes that OSC was not used to deliberately affect the group, by changing
    // grouping constraints, say, since the group structure is not on the master.
    // So it only deals with unintended consequences.
    private void rebuildGroup(AkibanInformationSchema ais, UserTable table,
                              Collection<TableChange> columnChanges) {
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            table.removeCandidateParentJoin(parentJoin);
            parentJoin.getParent().removeCandidateChildJoin(parentJoin);
            if (joinStillValid(parentJoin, table, columnChanges, false)) {
                parentJoin = rebuildJoin(ais, parentJoin, table, columnChanges, false);
                assert (table.getParentJoin() == parentJoin);
            }
            else {
                logger.info("Join {} no longer valid; group split.", parentJoin);
            }
        }
        for (Join childJoin : table.getChildJoins()) {
            table.removeCandidateChildJoin(parentJoin);
            childJoin.getChild().removeCandidateParentJoin(childJoin);
            if (joinStillValid(childJoin, table, columnChanges, true)) {
                childJoin = rebuildJoin(ais, childJoin, table, columnChanges, true);
            }
            else {
                logger.info("Join {} no longer valid; group split.", childJoin);
            }
        }
    }

    private boolean joinStillValid(Join join, UserTable table, 
                                   Collection<TableChange> columnChanges, boolean asParent) {
        for (JoinColumn joinColumn : join.getJoinColumns()) {
            Column column = (asParent) ? joinColumn.getParent() : joinColumn.getChild();
            assert (column.getTable() == table);
            if (correspondingJoinColumn(column, table, columnChanges) == null)
                return false;
        }
        return true;
    }
    
    private Join rebuildJoin(AkibanInformationSchema ais, Join oldJoin, UserTable table, 
                             Collection<TableChange> columnChanges, boolean asParent) {
        Join newJoin = Join.create(ais, oldJoin.getName(), oldJoin.getParent(), oldJoin.getChild());
        newJoin.setGroup(oldJoin.getGroup());
        for (JoinColumn joinColumn : oldJoin.getJoinColumns()) {
            Column parent = joinColumn.getParent();
            Column child = joinColumn.getChild();
            if (asParent)
                parent = correspondingJoinColumn(parent, table, columnChanges);
            else
                child = correspondingJoinColumn(child, table, columnChanges);
            newJoin.addJoinColumn(parent, child);
        }
        return newJoin;
    }

    private Column correspondingJoinColumn(Column column, UserTable table, Collection<TableChange> columnChanges) {
        String columnName = column.getName();
        for (TableChange change : columnChanges) {
            switch (change.getChangeType()) {
            case DROP:
                if (change.getOldName().equals(columnName)) {
                    return null;
                }
            case MODIFY:
                if (change.getOldName().equals(columnName)) {
                    columnName = change.getNewName();
                    break;
                }
            }
        }
        return table.getColumn(columnName);
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
        return ddl().getAIS(session);
    }

    private DDLFunctions ddl() {
        return requiredServices.dxl().ddlFunctions();
    }
}
