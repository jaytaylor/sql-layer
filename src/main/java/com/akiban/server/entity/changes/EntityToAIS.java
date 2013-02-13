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
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.Type;
import com.akiban.ais.model.Types;
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
        builder.addTableToGroup(groupName, schemaName, name);
    }

    @Override
    public void leaveEntity() {
        curTable = null;
        groupName = null;
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        String typeName = scalar.getType();
        Type type = builder.akibanInformationSchema().getType(typeName);
        Long params[] = getTypeParams(type, scalar.getProperties());
        String charAndCol[] = getCharAndCol(type, scalar.getProperties());
        boolean isNullable = !ATTR_REQUIRED_DEFAULT;
        boolean isAutoInc = false;
        if(scalar.isSpinal()) {
            isNullable = false;
            addSpinalColumn(name, scalar.getSpinePos());
        }
        Column column = builder.column(schemaName, curTable.name, name, nextColPos(),
                                       typeName, params[0], params[1],
                                       isNullable, isAutoInc,
                                       charAndCol[0], charAndCol[1]);
        column.setUuid(scalar.getUUID());
        visitScalarValidations(column, scalar.getValidation());
    }

    @Override
    public void visitCollection(String name, Attribute collection) {
        TableInfo parent = curTable;
        beginTable(name, collection.getUUID());
        parent.childTables.add(curTable);
    }

    @Override
    public void leaveCollection() {
        endTable();
    }

    @Override
    public void leaveEntityAttributes() {
        createPK(builder, schemaName, curTable);
        endTable();
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
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

    //
    // EntityToAIS
    //

    public AkibanInformationSchema getAIS() {
        return builder.akibanInformationSchema();
    }

    //
    // Helpers
    //

    private void addSpinalColumn(String name, int spinePos) {
        while(curTable.spinalCols.size() <= spinePos) {
            curTable.spinalCols.add(null);
        }
        curTable.spinalCols.set(spinePos, name);
    }

    private void beginTable(String name, UUID uuid) {
        UserTable table = builder.userTable(schemaName, name);
        table.setUuid(uuid);
        curTable = new TableInfo(name, table);
        tableInfoStack.add(curTable);
    }

    private void endTable() {
        // Create joins to children.
        // Parent spinal columns are automatically propagated to each child.
        if(curTable.spinalCols.isEmpty() && !curTable.childTables.isEmpty()) {
            throw new IllegalArgumentException("Has collections but no spine: " + curTable.name);
        }
        for(TableInfo child : curTable.childTables) {
            String joinName = child.name + "_" + curTable.name;
            builder.joinTables(joinName, schemaName, curTable.name, schemaName, child.name);

            for(String parentColName : curTable.spinalCols) {
                Column parentCol = curTable.table.getColumn(parentColName);
                String childColName = createColumnName(child.table.getColumns(), parentColName + "_ref");
                Column newCol = Column.create(child.table, parentCol, childColName, child.nextColPos++);
                // Should be exactly the same *except* UUID
                newCol.setUuid(null);
                builder.joinColumns(joinName,
                                    schemaName, curTable.name, parentColName,
                                    schemaName, child.name, childColName);
            }
        }
        if(tableInfoStack.size() == 1) {
            addJoinsToGroup(builder, groupName, curTable);
        }
        tableInfoStack.remove(tableInfoStack.size() - 1);
        curTable = tableInfoStack.isEmpty() ? null : tableInfoStack.get(tableInfoStack.size() - 1);
    }

    private int nextColPos() {
        return curTable.nextColPos++;
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

    private static void addJoinsToGroup(AISBuilder builder, TableName groupName, TableInfo curTable) {
        for(TableInfo child : curTable.childTables) {
            List<Join> joins = child.table.getCandidateParentJoins();
            assert joins.size() == 1 : joins;
            builder.addJoinToGroup(groupName, joins.get(0).getName(), 0);
            addJoinsToGroup(builder, groupName, child);
        }
    }

    private static String createColumnName(List<Column> curColumns, String proposed) {
        int offset = 1;
        String newName = proposed;
        for(int i = 0; i < curColumns.size(); ++i) {
            if(curColumns.get(i).getName().equals(newName)) {
                newName = proposed + "$" + offset++;
                i = -1;
            }
        }
        return newName;
    }

    private static void createPK(AISBuilder builder, String schemaName, TableInfo table) {
        if(!table.spinalCols.isEmpty()) {
            builder.index(schemaName, table.name, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            int pos = 0;
            for(String column : table.spinalCols) {
                builder.indexColumn(schemaName, table.name, Index.PRIMARY_KEY_CONSTRAINT, column, pos++, true, null);
            }
        }
        for(TableInfo child : table.childTables) {
            createPK(builder, schemaName, child);
        }
    }

    private static Long[] getTypeParams(Type type, Map<String,Object> props) {
        Long params[] = { null, null };
        if(type == Types.DECIMAL || type == Types.U_DECIMAL) {
            params[0] = maybeLong(props.get("precision"));
            params[1] = maybeLong(props.get("scale"));
        } else if(type == Types.CHAR || type == Types.VARCHAR || type == Types.BINARY || type == Types.VARBINARY) {
            params[0] = maybeLong(props.get("max_length"));
        }
        return params;
    }

    private static String[] getCharAndCol(Type type, Map<String,Object> props) {
        String charAndCol[] = { null, null };
        if(Types.isTextType(type)) {
            charAndCol[0] = maybeString(props.get("charset"));
            charAndCol[1] = maybeString(props.get("collation"));
        }
        return charAndCol;
    }

    private static boolean isGroupIndex(List<EntityColumn> columns) {
        for(int i = 1; i < columns.size(); ++i) {
            if(!columns.get(0).getTable().equals(columns.get(i).getTable())) {
                return true;
            }
        }
        return false;
    }

    private static Long maybeLong(Object o) {
        Number n = (Number)o;
        return (o != null) ? ((Number)o).longValue() : null;
    }

    private static String maybeString(Object o) {
        return (o != null) ? o.toString() : null;
    }


    private static class TableInfo {
        public final String name;
        public final UserTable table;
        public final List<String> spinalCols;
        public final List<TableInfo> childTables;
        public int nextColPos;

        public TableInfo(String name, UserTable table) {
            this.name = name;
            this.table = table;
            this.spinalCols = new ArrayList<>();
            this.childTables = new ArrayList<>();
        }

        @Override
        public String toString() {
            return name;
        }
    }
}