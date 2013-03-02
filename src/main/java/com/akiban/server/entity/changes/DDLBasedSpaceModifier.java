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

package com.akiban.server.entity.changes;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.NopVisitor;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.util.TableChange;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Space;
import com.akiban.server.entity.model.Validation;
import com.akiban.server.service.session.Session;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DDLBasedSpaceModifier implements SpaceModificationHandler {
    private final DDLFunctions ddlFunctions;
    private final Session session;
    private final String schemaName;
    private final SpaceLookups newSpaceLookup;
    private final AkibanInformationSchema oldAIS;
    private final AkibanInformationSchema newAIS;
    private final List<String> errors = new ArrayList<>();

    // Per entity change information. Tracked after beginEntity() and executed in endEntity()
    private final List<UserTable> dropTables = new ArrayList<>();
    private final List<String> dropGroupIndexes = new ArrayList<>();
    private final List<UserTable> newTables = new ArrayList<>();
    private final List<Index> newGroupIndexes = new ArrayList<>();
    private final Map<String,TableChangeInfo> tableChanges = new HashMap<>();

    // Current entity being visited.
    private Entity entity;
    private String entityOldName;
    private AttributeLookups oldLookups;
    private AttributeLookups newLookups;


    public DDLBasedSpaceModifier(DDLFunctions ddlFunctions, Session session, String schemaName, Space newSpace) {
        this.ddlFunctions = ddlFunctions;
        this.session = session;
        this.schemaName = schemaName;
        this.newSpaceLookup = new SpaceLookups(newSpace);
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
    public void addEntity(Entity entity, String name) {
        String entityName = newSpaceLookup.getName(entity.uuid());
        assert entityName.equals(name) : "mismatch between " + name + " and " + entityName;
        UserTable newRoot = newAIS.getUserTable(schemaName, entityName);
        createTableRecursively(newRoot);
        if(!newRoot.getGroup().getIndexes().isEmpty()) {
            ddlFunctions.createIndexes(session, newRoot.getGroup().getIndexes());
        }
    }

    @Override
    public void dropEntity(Entity dropped, String oldName) {
        ddlFunctions.dropGroup(session, new TableName(schemaName, oldName));
    }

    @Override
    public void beginEntity(UUID entityUUID) {
        entity = newSpaceLookup.getEntity(entityUUID);
        entityOldName = newSpaceLookup.getName(entityUUID);
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName) {
        assert entity.uuid().equals(entityUuid);
        entityOldName = oldName;
        String newName = newSpaceLookup.getName(entityUuid);
        trackTableRename(oldName, newName);
    }

    @Override
    public void beginAttributes(AttributeLookups oldLookups, AttributeLookups newLookups) {
        this.oldLookups = oldLookups;
        this.newLookups = newLookups;
    }

    @Override
    public void addAttribute(String attrName, Attribute attribute) {
        switch(attribute.getAttributeType()) {
            case SCALAR:
                UUID parent = newLookups.getParentAttribute(attribute.getUUID());
                trackColumnChange(getParentName(parent), TableChange.createAdd(attrName));
            break;
            case COLLECTION:
                newTables.add(newAIS.getUserTable(schemaName, attrName));
            break;
            default:
                unknownAttributeType(attribute);
        }
    }

    @Override
    public void dropAttribute(Attribute dropped) {
        UUID parent = oldLookups.getParentAttribute(dropped.getUUID());
        String oldName = oldLookups.nameFor(dropped.getUUID());
        switch(dropped.getAttributeType()) {
            case SCALAR:
                trackColumnChange(getParentName(parent), TableChange.createDrop(oldName));
            break;
            case COLLECTION:
                dropTables.add(ddlFunctions.getUserTable(session, new TableName(schemaName, oldName)));
            break;
            default:
                unknownAttributeType(dropped);
        }
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName) {
        UUID parent = newLookups.getParentAttribute(attributeUuid);
        Attribute attr = newLookups.attributeFor(attributeUuid);
        String newName = newLookups.nameFor(attributeUuid);
        switch(attr.getAttributeType()) {
            case SCALAR:
                trackColumnChange(getParentName(parent), TableChange.createModify(oldName, newName));
            break;
            case COLLECTION:
                trackTableRename(oldName, newName);
            break;
            default:
                unknownAttributeType(attr);
        }
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange) {
        trackColumnModify(scalarUuid);
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange) {
        trackColumnModify(scalarUuid);
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange) {
        trackColumnModify(scalarUuid);
    }

    @Override
    public void endAttributes() {
        oldLookups = null;
        newLookups = null;
    }

    @Override
    public void addEntityValidation(Validation validation) {
        errors.add("Adding entity validations is not yet supported: " + validation);
    }

    @Override
    public void dropEntityValidation(Validation validation) {
        errors.add("Dropping entity validations is not yet supported: " + validation);
    }

    @Override
    public void addIndex(String name) {
        String entityName = newSpaceLookup.getName(entity.uuid());
        UserTable rootTable = newAIS.getUserTable(schemaName, entityName);
        Index candidate = findOneIndex(rootTable, name);
        if(candidate.isGroupIndex()) {
            newGroupIndexes.add(candidate);
        } else {
            // ALTER processing takes care of table index changes automatically, just need to make sure it is called.
            getChangeSet(candidate.leafMostTable().getName().getTableName());
        }
    }

    @Override
    public void dropIndex(String name, EntityIndex index) {
        UserTable oldTable = oldAIS.getUserTable(schemaName, entityOldName);
        Index candidate = findOneIndex(oldTable, name);
        if(candidate.isGroupIndex()) {
            dropGroupIndexes.add(name);
        } else {
            String table = candidate.leafMostTable().getName().getTableName();
            trackIndexChange(table, TableChange.createDrop(candidate.getIndexName().getName()));
        }
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName) {
        errors.add("Renaming index is not yet supported: " + oldName + "=>" + newName);
    }

    @Override
    public void endEntity() {
        if(!errors.isEmpty()) {
            resetPerEntityData();
            return;
        }

        if(!dropGroupIndexes.isEmpty()) {
            TableName oldGroupName = new TableName(schemaName, entityOldName);
            ddlFunctions.dropGroupIndexes(session, oldGroupName, dropGroupIndexes);
        }
        for(UserTable table : dropTables) {
            dropTableRecursively(table);
        }
        for(UserTable table : newTables) {
            createTableRecursively(table);
        }
        UserTable oldRoot = oldAIS.getUserTable(schemaName, entityOldName);
        oldRoot.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                TableName oldName = table.getName();
                TableChangeInfo changeInfo = tableChanges.get(oldName.getTableName());
                if(changeInfo != null) {
                    UserTable newDef = newAIS.getUserTable(new TableName(schemaName, changeInfo.newName));
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

    private void createTableRecursively(UserTable startTable) {
        ddlFunctions.createTable(session, startTable);
        for(Join child : startTable.getChildJoins()) {
            createTableRecursively(child.getChild());
        }
    }

    private void dropTableRecursively(UserTable startTable) {
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

    private String getParentName(UUID parentUuid) {
        return (parentUuid == null) ? entityOldName : oldLookups.nameFor(parentUuid);
    }

    private void resetPerEntityData() {
        entity = null;
        entityOldName = null;
        newLookups = null;
        dropTables.clear();
        dropGroupIndexes.clear();
        newTables.clear();
        newGroupIndexes.clear();
        tableChanges.clear();
    }

    private void trackColumnChange(String tableName, TableChange columnChange) {
        getChangeSet(tableName).columnChanges.add(columnChange);
    }

    private void trackColumnModify(UUID columnUuid) {
        UUID parent = newLookups.getParentAttribute(columnUuid);
        String oldName = oldLookups.nameFor(columnUuid);
        String newName = newLookups.nameFor(columnUuid);
        trackColumnChange(getParentName(parent), TableChange.createModify(oldName, newName));
    }

    private void trackIndexChange(String tableName, TableChange indexChange) {
        getChangeSet(tableName).indexChanges.add(indexChange);
    }

    private void trackTableRename(String oldTableName, String newTableName) {
        getChangeSet(oldTableName).newName = newTableName;
    }

    private static Index findOneIndex(UserTable root, final String indexName) {
        final List<Index> candidates = new ArrayList<>();
        root.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
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

    private static void unknownAttributeType(Attribute a) {
        throw new IllegalStateException("Unknown attribute type: " + a);
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
