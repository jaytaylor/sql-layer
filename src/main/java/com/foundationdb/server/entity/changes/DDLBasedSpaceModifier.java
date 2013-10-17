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

package com.foundationdb.server.entity.changes;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.NopVisitor;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.entity.model.Entity;
import com.foundationdb.server.entity.model.EntityCollection;
import com.foundationdb.server.entity.model.EntityIndex;
import com.foundationdb.server.entity.model.Space;
import com.foundationdb.server.entity.model.Validation;
import com.foundationdb.server.service.session.Session;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DDLBasedSpaceModifier extends AbstractSpaceModificationHandler {
    private final DDLFunctions ddlFunctions;
    private final Session session;
    private final String schemaName;
    private final AkibanInformationSchema oldAIS;
    private final AkibanInformationSchema newAIS;
    private final List<String> errors = new ArrayList<>();

    // Per entity change information. Tracked after beginEntity() and executed in endEntity()
    private final List<String> dropGroupIndexes = new ArrayList<>();
    private final List<Index> newGroupIndexes = new ArrayList<>();
    private final Map<String,TableChangeInfo> tableChanges = new HashMap<>();

    // Current entity being visited.
    private int level;
    private Entity oldEntity;
    private Entity newEntity;
    private Entity oldTopEntity;


    public DDLBasedSpaceModifier(DDLFunctions ddlFunctions, Session session, String schemaName, Space newSpace) {
        this.ddlFunctions = ddlFunctions;
        this.session = session;
        this.schemaName = schemaName;
        this.oldAIS = ddlFunctions.getAIS(session);
        EntityToAIS eToAIS = new EntityToAIS(schemaName);
        newSpace.visit(eToAIS);
        this.newAIS = eToAIS.getAIS();
    }

    //
    // DDLBasedSpaceModifier
    //

    public boolean hadError() {
        return !errors.isEmpty();
    }

    public List<String> getErrors() {
        return errors;
    }

    //
    // SpaceModificationHandler
    //

    @Override
    public void addEntity(Entity newEntity) {
        if (newEntity instanceof EntityCollection) {
            Table table = newAIS.getTable(schemaName, newEntity.getName());
            createTableRecursively(table);
        }
        else {
            Table newRoot = newAIS.getTable(schemaName, newEntity.getName());
            createTableRecursively(newRoot);
            if(!newRoot.getGroup().getIndexes().isEmpty()) {
                ddlFunctions.createIndexes(session, newRoot.getGroup().getIndexes());
            }
        }
    }

    @Override
    public void dropEntity(Entity dropped) {
        if (dropped instanceof EntityCollection) {
            Table table = ddlFunctions.getTable(session, new TableName(schemaName, dropped.getName()));
            dropTableRecursively(table);
        }
        else {
            ddlFunctions.dropGroup(session, new TableName(schemaName, dropped.getName()));
        }
    }

    @Override
    public void beginEntity(Entity oldEntity, Entity newEntity) {
        if (level++ == 0) {
            assert oldTopEntity == null : oldEntity;
            oldTopEntity = oldEntity;
        }
        this.oldEntity = oldEntity;
        this.newEntity = newEntity;
    }

    @Override
    public void renameEntity() {
        trackTableRename(oldEntity.getName(), newEntity.getName());
    }

    @Override
    public void addField(UUID added) {
        String name = getFieldName(added, newEntity);
        trackColumnChange(newEntity.getName(), TableChange.createAdd(name));
    }

    @Override
    public void dropField(UUID dropped) {
        String oldName = getFieldName(dropped, oldEntity);
        trackColumnChange(oldEntity.getName(), TableChange.createDrop(oldName));
    }

    @Override
    public void renameField(UUID fieldUuid) {
        String oldName = getFieldName(fieldUuid, oldEntity);
        String newName = getFieldName(fieldUuid, newEntity);
        trackColumnChange(oldEntity.getName(), TableChange.createModify(oldName, newName));
    }

    @Override
    public void fieldOrderChanged(UUID fieldUuid) {
        String oldName = getFieldName(fieldUuid, oldEntity);
        String newName = getFieldName(fieldUuid, newEntity);
        trackColumnChange(oldEntity.getName(), TableChange.createModify(oldName, newName));
    }

    @Override
    public void changeFieldType(UUID fieldUuid) {
        trackColumnModify(fieldUuid);
    }

    @Override
    public void changeFieldValidations(UUID fieldUuid) {
        trackColumnModify(fieldUuid);
    }

    @Override
    public void changeFieldProperties(UUID fieldUuid) {
        trackColumnModify(fieldUuid);
    }

    @Override
    public void addIndex(String name) {
        String entityName = newEntity.getName();
        Table rootTable = newAIS.getTable(schemaName, entityName);
        Index candidate = findOneIndex(rootTable, name);
        if(candidate.isGroupIndex()) {
            newGroupIndexes.add(candidate);
        } else {
            // ALTER processing takes care of table index changes automatically, just need to make sure it is called.
            getChangeSet(candidate.leafMostTable().getName().getTableName());
        }
    }

    @Override
    public void dropIndex(String name) {
        Table oldTable = oldAIS.getTable(schemaName, oldEntity.getName());
        Index candidate = findOneIndex(oldTable, name);
        if(candidate.isGroupIndex()) {
            dropGroupIndexes.add(name);
        } else {
            String table = candidate.leafMostTable().getName().getTableName();
            trackIndexChange(table, TableChange.createDrop(candidate.getIndexName().getName()));
        }
    }

    @Override
    public void endEntity() {
        if (--level != 0)
            return;
        if(!errors.isEmpty()) {
            resetPerEntityData();
            return;
        }

        if(!dropGroupIndexes.isEmpty()) {
            TableName oldGroupName = new TableName(schemaName, oldTopEntity.getName());
            ddlFunctions.dropGroupIndexes(session, oldGroupName, dropGroupIndexes);
        }
        Table oldRoot = oldAIS.getTable(schemaName, oldTopEntity.getName());
        oldRoot.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitTable(Table table) {
                TableName oldName = table.getName();
                TableChangeInfo changeInfo = tableChanges.get(oldName.getTableName());
                if(changeInfo != null) {
                    Table newDef = newAIS.getTable(new TableName(schemaName, changeInfo.newName));
                    ddlFunctions.alterTable(session, oldName, newDef, changeInfo.columnChanges, changeInfo.indexChanges, null);
                }
            }
        });
        if(!newGroupIndexes.isEmpty()) {
            ddlFunctions.createIndexes(session, newGroupIndexes);
        }
        resetPerEntityData();
    }

    @Override
    public void error(String message) {
        errors.add(message);
    }

    //
    // Helpers
    //

    private void createTableRecursively(Table startTable) {
        ddlFunctions.createTable(session, startTable);
        for(Join child : startTable.getChildJoins()) {
            createTableRecursively(child.getChild());
        }
    }

    private void dropTableRecursively(Table startTable) {
        for(Join child : startTable.getChildJoins()) {
            dropTableRecursively(child.getChild());
        }
        ddlFunctions.dropTable(session, startTable.getName());
    }

    private TableChangeInfo getChangeSet(String tableName) {
        TableChangeInfo changeSet = tableChanges.get(tableName);
        if(changeSet == null) {
            changeSet = new TableChangeInfo(tableName);
            tableChanges.put(tableName, changeSet);
        }
        return changeSet;
    }

    private static String getFieldName(UUID fieldUuid, Entity entity) {
        return entity.fieldsByUuid().get(fieldUuid).getName();
    }

    private void resetPerEntityData() {
        oldTopEntity = null;
        oldEntity = null;
        newEntity = null;
        dropGroupIndexes.clear();
        newGroupIndexes.clear();
        tableChanges.clear();
    }

    private void trackColumnChange(String tableName, TableChange columnChange) {
        getChangeSet(tableName).columnChanges.add(columnChange);
    }

    private void trackColumnModify(UUID columnUuid) {
        String oldName = getFieldName(columnUuid, oldEntity);
        String newName = getFieldName(columnUuid, newEntity);
        trackColumnChange(oldEntity.getName(), TableChange.createModify(oldName, newName));
    }

    private void trackIndexChange(String tableName, TableChange indexChange) {
        getChangeSet(tableName).indexChanges.add(indexChange);
    }

    private void trackTableRename(String oldTableName, String newTableName) {
        getChangeSet(oldTableName).newName = newTableName;
    }

    private static Index findOneIndex(Table root, final String indexName) {
        final List<Index> candidates = new ArrayList<>();
        root.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitTable(Table table) {
                for(Index index : table.getIndexes()) {
                    if(index.getIndexName().getName().equals(indexName)) {
                        candidates.add(index);
                    }
                }
            }
        });
        for(Index index : root.getGroupIndexes()) {
            if(index.getIndexName().getName().equals(indexName)) {
                candidates.add(index);
            }
        }
        if(candidates.size() != 1) {
            throw new IllegalStateException("Could not find exact index " + indexName + ": " + candidates);
        }
        return candidates.get(0);
    }

    private static class TableChangeInfo {
        public String newName;
        public final List<TableChange> columnChanges = new ArrayList<>();
        public final List<TableChange> indexChanges = new ArrayList<>();

        public TableChangeInfo(String name) {
            newName = name;
        }
    }
}
