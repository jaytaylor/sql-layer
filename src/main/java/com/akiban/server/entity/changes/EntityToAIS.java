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

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.server.entity.model.AbstractEntityVisitor;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityColumn;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import com.google.common.collect.BiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class EntityToAIS extends AbstractEntityVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(EntityToAIS.class);

    private static final boolean ATTR_REQUIRED_DEFAULT = true;
    private static final Index.JoinType GI_JOIN_TYPE_DEFAULT = Index.JoinType.LEFT;

    private final String schemaName;
    private final AISBuilder builder = new AISBuilder();
    private final List<TableInfo> tableInfoStack = new ArrayList<>();
    private TableName groupName = null;
    private TableInfo curTable = null;

    public EntityToAIS(String schemaName) {
        this.schemaName = schemaName;
    }

    //
    // EntityVisitor
    //

    @Override
    public void visitEntity(String name, Entity entity) {
        builder.createGroup(name, schemaName);
        groupName = new TableName(schemaName, name);
        beginTable(name, entity.uuid());
    }

    @Override
    public void leaveEntity() {
        endTable();
        groupName = null;
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        // TODO: Lookup properties based on type
        Map<String,Object> props = scalar.getProperties();
        String charset = (String)props.get("charset");
        String collation = (String)props.get("collation");
        Long param1 = (Long)props.get("max_length");
        if(param1 == null) {
            param1 = (Long)props.get("precision");
        }
        Long param2 = (Long)props.get("scale");
        Column column = builder.column(schemaName,
                                       curTableName(),
                                       name,
                                       nextColPos(),
                                       scalar.getType(),
                                       param1,
                                       param2,
                                       !ATTR_REQUIRED_DEFAULT, /*nullable*/
                                       false, /*autoinc*/
                                       charset,
                                       collation
                                       /*,default*/);
        column.setUuid(scalar.getUUID());

        visitScalarValidations(column, scalar.getValidation());

        if(scalar.isSpinal()) {
            addSpinalColumn(name, scalar.getSpinePos());
        }
    }

    private void visitScalarValidations(Column column, Collection<Validation> validations) {
        for(Validation v : validations) {
            if("required".equals(v.getName())) {
                boolean isRequired = (Boolean)v.getValue();
                column.setNullable(!isRequired);
            } else {
                LOG.warn("Ignored scalar validation on table {}: {}", curTable, v);
            }
        }
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
        curTable.childTables.add(name);
        beginTable(name, collection.getUUID());
    }

    @Override
    public void leaveCollection() {
        endTable();
    }

    @Override
    public void leaveEntityAttributes() {
        // None
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
        for(Validation v : validations) {
            if("unique".equals(v.getName())) {
                List<List<String>> colNames = (List<List<String>>)v.getValue();
                String firstTable = colNames.get(0).get(0);
                String indexName = colNames.get(0).get(1); // TODO: Generate as required
                builder.index(schemaName, firstTable, indexName, true, Index.UNIQUE_KEY_CONSTRAINT);
                for(int i = 0; i < colNames.size(); ++i) {
                    List<String> pair = colNames.get(i);
                    if(!pair.get(0).equals(firstTable)) {
                        throw new IllegalArgumentException("Multi-table unique index");
                    }
                    builder.indexColumn(schemaName, pair.get(0), indexName, pair.get(1), i, true, null);
                }
            } else {
                throw new IllegalArgumentException("Unknown validation: " + v.getName());
            }
        }
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
        for(Map.Entry<String,EntityIndex> entry : indexes.entrySet()) {
            String indexName = entry.getKey();
            List<EntityColumn> columns = entry.getValue().getColumns();
            boolean isGI = isGroupIndex(columns);
            if(isGI) {
                TableName groupName = new TableName(schemaName, tableInfoStack.get(0).name);
                builder.groupIndex(groupName, indexName, false, GI_JOIN_TYPE_DEFAULT);
                int pos = 0;
                for(EntityColumn col : columns) {
                    builder.groupIndexColumn(groupName, indexName, schemaName, col.getTable(), col.getColumn(), pos++);
                }
            } else {
                builder.index(schemaName, columns.get(0).getTable(), indexName, false, Index.KEY_CONSTRAINT);
                int pos = 0;
                for(EntityColumn col : columns) {
                    builder.indexColumn(schemaName, col.getTable(), indexName, col.getColumn(), pos++, true, null);
                }
            }
        }
    }

    private boolean isGroupIndex(List<EntityColumn> columns) {
        for(int i = 1; i < columns.size(); ++i) {
            if(!columns.get(0).getTable().equals(columns.get(i).getTable())) {
                return true;
            }
        }
        return false;
    }

    //
    // AISBuilderVisitor
    //

    public AkibanInformationSchema getAIS() {
        return builder.akibanInformationSchema();
    }

    private void beginTable(String name, UUID uuid) {
        curTable = new TableInfo(name);
        UserTable table = builder.userTable(schemaName, name);
        table.setUuid(uuid);
        builder.addTableToGroup(groupName, schemaName, name);
        tableInfoStack.add(curTable);
    }

    private void endTable() {
        // Create PRIMARY
        if(!curTable.spinalCols.isEmpty()) {
            builder.index(schemaName, curTableName(), Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            int pos = 0;
            for(String column : curTable.spinalCols) {
                builder.indexColumn(schemaName, curTableName(), Index.PRIMARY_KEY_CONSTRAINT, column, pos++, true, null);
            }
        }

        // Create joins to children
        List<IndexColumn> parentPK = getPKCols(curTableName());
        if(parentPK == null) {
            throw new IllegalArgumentException("Table has children but no PK: " + curTableName());
        }
        for(String childName : curTable.childTables) {
            List<IndexColumn> childPK = getPKCols(childName);
            if(childPK == null) {
                throw new IllegalArgumentException("Child has no PK: " + childName);
            }

            String joinName = childName + "_" + curTableName();
            builder.joinTables(joinName, schemaName, curTableName(), schemaName, childName);
            builder.addJoinToGroup(groupName, joinName, 0);

            int max = Math.min(parentPK.size(), childPK.size());
            for(int i = 0; i < max; ++i) {
                builder.joinColumns(joinName,
                                    schemaName, curTableName(), parentPK.get(i).getColumn().getName(),
                                    schemaName, childName, childPK.get(i).getColumn().getName());
            }
        }

        // Reset current
        tableInfoStack.remove(tableInfoStack.size() - 1);
        curTable = tableInfoStack.isEmpty() ? null : tableInfoStack.get(tableInfoStack.size() - 1);
    }

    private List<IndexColumn> getPKCols(String tableName) {
        UserTable table = builder.akibanInformationSchema().getUserTable(schemaName, tableName);
        if(table.getPrimaryKey() == null) {
            return null;
        }
        return table.getPrimaryKey().getIndex().getKeyColumns();
    }

    private int nextColPos() {
        return curTable.nextColPos++;
    }

    private String curTableName() {
        return curTable.name;
    }

    private void addSpinalColumn(String name, int spinePos) {
        while(curTable.spinalCols.size() <= spinePos) {
            curTable.spinalCols.add(null);
        }
        curTable.spinalCols.set(spinePos, name);
    }

    private static class TableInfo {
        public final String name;
        public final List<String> spinalCols;
        public final List<String> childTables;
        public int nextColPos;

        public TableInfo(String name) {
            this.name = name;
            this.spinalCols = new ArrayList<>();
            this.childTables = new ArrayList<>();
        }
    }
}