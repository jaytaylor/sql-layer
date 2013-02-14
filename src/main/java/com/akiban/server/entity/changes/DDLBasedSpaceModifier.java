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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class DDLBasedSpaceModifier implements SpaceModificationHandler {
    private final DDLFunctions ddlFunctions;
    private final Session session;
    private final String schemaName;
    private final Space space;
    private final SpaceLookups spaceLookups;
    private final AkibanInformationSchema newAIS;

    private final List<UserTable> newTables = new ArrayList<>();
    private final List<String> dropTables = new ArrayList<>();
    private final List<Index> newGroupIndex = new ArrayList<>();
    private final List<String> dropGroupIndex = new ArrayList<>();
    private final Map<String,TableChangeSet> tableChanges = new HashMap<>();

    private Entity entity;
    private AttributeLookups oldLookups;
    private AttributeLookups newLookups;


    public DDLBasedSpaceModifier(DDLFunctions ddlFunctions, Session session, String schemaName, Space space) {
        this.ddlFunctions = ddlFunctions;
        this.session = session;
        this.schemaName = schemaName;
        this.space = space;
        this.spaceLookups = new SpaceLookups(space);
        EntityToAIS eToAIS = new EntityToAIS(schemaName);
        space.visit(eToAIS);
        this.newAIS = eToAIS.getAIS();
    }

    @Override
    public void beginEntity(UUID entityUUID) {
        entity = spaceLookups.getEntity(entityUUID);
    }

    @Override
    public void endEntity() {
        Collections.sort(dropTables, new Comparator<String>() {
            final AkibanInformationSchema oldAIS = ddlFunctions.getAIS(session);
            @Override
            public int compare(String o1, String o2) {
                UserTable t1 = oldAIS.getUserTable(schemaName, o1);
                UserTable t2 = oldAIS.getUserTable(schemaName, o2);
                return t1.getDepth().compareTo(t2.getDepth());
            }
        });

        for(String name : dropTables) {
            ddlFunctions.dropTable(session, new TableName(schemaName, name));
        }

        for(UserTable table : newTables) {
            createTableRecursively(table);
        }

        for(Map.Entry<String,TableChangeSet> entry : tableChanges.entrySet()) {
            TableName oldName = new TableName(schemaName, entry.getKey());
            UserTable newDef = newAIS.getUserTable(new TableName(schemaName, entry.getValue().newName));
            ddlFunctions.alterTable(session, oldName, newDef, entry.getValue().columnChanges, entry.getValue().indexChanges, null);
        }

        if(!newGroupIndex.isEmpty()) {
            ddlFunctions.createIndexes(session, newGroupIndex);
        }

        if(!dropGroupIndex.isEmpty()) {
            TableName groupName = new TableName(schemaName, spaceLookups.getName(entity.uuid()));
            ddlFunctions.dropGroupIndexes(session, groupName, dropGroupIndex);
        }

        entity = null;
        newLookups = null;
        newTables.clear();
        dropTables.clear();
        newGroupIndex.clear();
        dropGroupIndex.clear();

        tableChanges.clear();
    }

    @Override
    public void addEntity(UUID entityUuid) {
        String entityName = spaceLookups.getName(entityUuid);
        UserTable root = newAIS.getUserTable(schemaName, entityName);
        createTableRecursively(root);
        if(!root.getGroup().getIndexes().isEmpty()) {
            ddlFunctions.createIndexes(session, root.getGroup().getIndexes());
        }
    }

    @Override
    public void dropEntity(Entity dropped, String oldName) {
        ddlFunctions.dropGroup(session, new TableName(schemaName, oldName));
    }

    @Override
    public void renameEntity(UUID entityUuid, String oldName) {
        String newName = spaceLookups.getName(entityUuid);
        ddlFunctions.renameTable(session, new TableName(schemaName, oldName), new TableName(schemaName, newName));
    }

    @Override
    public void beginAttributes(AttributeLookups oldLookups, AttributeLookups newLookups) {
        this.oldLookups = oldLookups;
        this.newLookups = newLookups;
    }

    @Override
    public void addAttribute(UUID parentAttributeUuid, UUID attributeUuid) {
        Attribute attr = newLookups.attributeFor(attributeUuid);
        String parentName = (parentAttributeUuid == null) ? getCurEntityName() : newLookups.nameFor(parentAttributeUuid);
        String attrName = newLookups.nameFor(attributeUuid);
        switch(attr.getAttributeType()) {
            case SCALAR:
                trackColumnChange(parentName, TableChange.createAdd(attrName));
            break;
            case COLLECTION:
                newTables.add(newAIS.getUserTable(schemaName, attrName));
            break;
            default:
                assert false : attr;
        }
    }

    @Override
    public void dropAttribute(UUID parentAttributeUuid, String oldName, Attribute dropped) {
        String parentName = (parentAttributeUuid == null) ? getCurEntityName() : newLookups.nameFor(parentAttributeUuid);
        switch(dropped.getAttributeType()) {
            case SCALAR:
                trackColumnChange(parentName, TableChange.createDrop(oldName));
            break;
            case COLLECTION:
                dropTables.add(oldName);
            break;
            default:
                assert false : dropped;
        }
    }

    @Override
    public void renameAttribute(UUID attributeUuid, String oldName) {
        UUID parent = newLookups.getParentAttribute(attributeUuid);
        Attribute attr = newLookups.attributeFor(attributeUuid);
        String parentName = (parent == null) ? getCurEntityName() : oldLookups.nameFor(parent);
        String newName = newLookups.nameFor(attributeUuid);
        switch(attr.getAttributeType()) {
            case SCALAR:
                trackColumnChange(parentName, TableChange.createModify(oldName, newName));
            break;
            case COLLECTION:
                trackTableRename(oldName, newName);
            break;
            default:
                assert false : attr;
        }
    }

    @Override
    public void changeScalarType(UUID scalarUuid, Attribute afterChange) {
        handleColumnChange(scalarUuid);
    }

    @Override
    public void changeScalarValidations(UUID scalarUuid, Attribute afterChange) {
        handleColumnChange(scalarUuid);
    }

    @Override
    public void changeScalarProperties(UUID scalarUuid, Attribute afterChange) {
        handleColumnChange(scalarUuid);
    }

    @Override
    public void endAttributes() {
        oldLookups = null;
        newLookups = null;
    }

    @Override
    public void addEntityValidation(Validation validation) {
        // TODO: No way to drop UNIQUE from an existing index
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropEntityValidation(Validation validation) {
        // TODO: No way to add UNIQUE to an existing index
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIndex(String name) {
        String entityName = spaceLookups.getName(entity.uuid());
        UserTable rootTable = newAIS.getUserTable(schemaName, entityName);
        Index candidate = findIndex(rootTable, name);
        if(candidate.isGroupIndex()) {
            newGroupIndex.add(candidate);
        } else {
            String table = candidate.leafMostTable().getName().getTableName();
            trackIndexChange(table, TableChange.createAdd(candidate.getIndexName().getName()));
        }
    }

    @Override
    public void dropIndex(String name, EntityIndex index) {
        String entityName = spaceLookups.getName(entity.uuid());
        UserTable rootTable = ddlFunctions.getAIS(session).getUserTable(schemaName, entityName);
        Index candidate = findIndex(rootTable, name);
        if(candidate.isGroupIndex()) {
            dropGroupIndex.add(name);
        } else {
            String table = candidate.leafMostTable().getName().getTableName();
            trackIndexChange(table, TableChange.createDrop(candidate.getIndexName().getName()));
        }
    }

    @Override
    public void renameIndex(EntityIndex index, String oldName, String newName) {
        // TODO: Index renaming might work for tables but no exposed way for GIs, defer for now
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void error(String message) {
        throw new UnsupportedOperationException(message);
    }

    private void handleColumnChange(UUID columnUuid) {
        UUID parent = newLookups.getParentAttribute(columnUuid);
        String parentName = (parent == null) ? getCurEntityName() : newLookups.nameFor(parent);
        String name = newLookups.nameFor(columnUuid);
        trackColumnChange(parentName, TableChange.createModify(name, name));
    }

    private static Index findIndex(UserTable root, final String indexName) {
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

    private String getCurEntityName() {
        return spaceLookups.getName(entity.uuid());
    }

    private TableChangeSet getChangeSet(String tableName) {
        TableChangeSet changeSet = tableChanges.get(tableName);
        if(changeSet == null) {
            changeSet = new TableChangeSet(tableName);
            tableChanges.put(tableName, changeSet);
        }
        return changeSet;
    }

    private void trackTableRename(String oldTableName, String newTableName) {
        getChangeSet(oldTableName).newName = newTableName;
    }

    private void trackColumnChange(String tableName, TableChange columnChange) {
        getChangeSet(tableName).columnChanges.add(columnChange);
    }

    private void trackIndexChange(String tableName, TableChange indexChange) {
        getChangeSet(tableName).indexChanges.add(indexChange);
    }

    private void createTableRecursively(UserTable root) {
        root.traverseTableAndDescendants(new NopVisitor() {
            @Override
            public void visitUserTable(UserTable table) {
                ddlFunctions.createTable(session, table);
            }
        });
    }

    private static class TableChangeSet {
        public String newName;
        public final List<TableChange> columnChanges = new ArrayList<>();
        public final List<TableChange> indexChanges = new ArrayList<>();

        public TableChangeSet(String name) {
            newName = name;
        }
    }
}
