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

package com.foundationdb.server.service.dxl;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.GroupIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.ais.util.TableChange.ChangeType;
import com.foundationdb.server.service.session.Session;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.foundationdb.ais.util.TableChangeValidator.TableColumnNames;

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

    public Integer findOldPosition(Table oldTable, Column newColumn) {
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

    public List<Index> findNewIndexesToBuild(Table newTable) {
        List<Index> indexes = new ArrayList<>();
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

    public void dropAffectedGroupIndexes(Session session, BasicDDLFunctions ddl, Table origTable) {
        // Drop definition and rebuild later, probably better than doing each entry individually
        if(affectedGroupIndexes.isEmpty()) {
            return;
        }
        List<GroupIndex> groupIndexes = new ArrayList<>();
        for(IndexName name : affectedGroupIndexes.keySet()) {
            groupIndexes.add(origTable.getGroup().getIndex(name.getName()));
        }
        ddl.schemaManager().dropIndexes(session, groupIndexes);
    }

    public void createAffectedGroupIndexes(Session session, BasicDDLFunctions ddl, Table origTable, Table newTable, boolean dataChange) {
        // Ideally only would copy the Group, but that is vulnerable to changing group names. Even if we handle that
        // by looking up the new name, index creation in PSSM requires index.getName().getTableName() match the actual.
        AkibanInformationSchema tempAIS = ddl.getAISCloner().clone(newTable.getAIS());
        List<Index> indexesToBuild = new ArrayList<>();
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
                Table tempTable = tempAIS.getTable(tcn.tableName);
                Column tempColumn = tempTable.getColumn(tcn.newColumnName);
                IndexColumn.create(tempIndex,  tempColumn, i, true, null);
            }
            tempIndex.copyStorageDescription(origIndex);
            indexesToBuild.add(tempIndex);
        }

        if(dataChange) {
            ddl.createIndexesInternal(session, indexesToBuild);
        } else {
            // Restore old trees
            ddl.schemaManager().createIndexes(session, indexesToBuild, true);
        }
    }
}
