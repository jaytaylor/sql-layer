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
import java.util.HashSet;
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
    private Set<String> uniqueValidations = new HashSet<>();

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
        uniqueValidations.clear();
    }

    @Override
    public void visitScalar(String name, Attribute scalar) {
        String typeName = scalar.getType();
        ColumnInfo info = getColumnInfo(builder.akibanInformationSchema().getType(typeName),
                                        scalar.getProperties(),
                                        scalar.getValidation());
        if(scalar.isSpinal()) {
            info.nullable = false;
            addSpinalColumn(name, scalar.getSpinePos());
        }
        Column column = builder.column(schemaName, curTable.name,
                                       name, curTable.nextColPos++,
                                       scalar.getType(), info.param1, info.param2,
                                       info.nullable, false /*isAutoInc*/,
                                       info.charset, info.collation);
        column.setUuid(scalar.getUUID());
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
        endTable();
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
    }

    @Override
    public void visitEntityValidations(Set<Validation> validations) {
        for(Validation v : validations) {
            if("unique".equals(v.getName())) {
                String indexName = (String)v.getValue();
                uniqueValidations.add(indexName);
            } else {
                LOG.warn("Ignored entity validation on {}: {}", groupName, v);
            }
        }
    }

    @Override
    public void visitIndexes(BiMap<String, EntityIndex> indexes) {
        for(Map.Entry<String,EntityIndex> entry : indexes.entrySet()) {
            String indexName = entry.getKey();
            List<EntityColumn> columns = entry.getValue().getColumns();
            boolean isGI = isMultiTable(columns);
            boolean isUnique = uniqueValidations.contains(indexName);
            if(isGI) {
                if(isUnique) {
                    throw new IllegalArgumentException("Unique group index not allowed");
                }
                builder.groupIndex(groupName, indexName, false, GI_JOIN_TYPE_DEFAULT);
                int pos = 0;
                for(EntityColumn col : columns) {
                    builder.groupIndexColumn(groupName, indexName, schemaName, col.getTable(), col.getColumn(), pos++);
                }
            } else {
                builder.index(schemaName, columns.get(0).getTable(), indexName, isUnique, Index.KEY_CONSTRAINT);
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
            // Create PKs at the end (root to leaf) so IDs are ordered as such. Shouldn't matter but is safe.
            createPrimaryKeys(builder, schemaName, curTable);
            addJoinsToGroup(builder, groupName, curTable);
        }
        tableInfoStack.remove(tableInfoStack.size() - 1);
        curTable = tableInfoStack.isEmpty() ? null : tableInfoStack.get(tableInfoStack.size() - 1);
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

    private static void createPrimaryKeys(AISBuilder builder, String schemaName, TableInfo table) {
        if(!table.spinalCols.isEmpty()) {
            builder.index(schemaName, table.name, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            int pos = 0;
            for(String column : table.spinalCols) {
                builder.indexColumn(schemaName, table.name, Index.PRIMARY_KEY_CONSTRAINT, column, pos++, true, null);
            }
        }
        for(TableInfo child : table.childTables) {
            createPrimaryKeys(builder, schemaName, child);
        }
    }

    private static ColumnInfo getColumnInfo(Type type, Map<String,Object> props, Collection<Validation> validations) {
        ColumnInfo info = new ColumnInfo();
        if(type == Types.DECIMAL || type == Types.U_DECIMAL) {
            info.param1 = maybeLong(props.get("precision"));
            info.param2 = maybeLong(props.get("scale"));
        }
        if(Types.isTextType(type)) {
            info.charset = maybeString(props.get("charset"));
            info.collation = maybeString(props.get("collation"));
        }
        for(Validation v : validations) {
            if("required".equals(v.getName())) {
                boolean isRequired = (Boolean)v.getValue();
                info.nullable = !isRequired;
            } else if("max_length".equals(v.getName())) {
                info.param1 = maybeLong(v.getValue());
            } else {
                LOG.warn("Ignored scalar validation on table: {}", v);
            }
        }
        return info;
    }

    private static boolean isMultiTable(List<EntityColumn> columns) {
        for(int i = 1; i < columns.size(); ++i) {
            if(!columns.get(0).getTable().equals(columns.get(i).getTable())) {
                return true;
            }
        }
        return false;
    }

    private static Long maybeLong(Object o) {
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

    private static class ColumnInfo {
        public Long param1;
        public Long param2;
        public String charset;
        public String collation;
        public boolean nullable = !ATTR_REQUIRED_DEFAULT;
    }
}