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

import com.foundationdb.ais.model.AISBuilder;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CharsetAndCollation;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Type;
import com.foundationdb.ais.model.Types;
import com.foundationdb.ais.model.UserTable;
import com.foundationdb.server.entity.model.Entity;
import com.foundationdb.server.entity.model.EntityCollection;
import com.foundationdb.server.entity.model.EntityField;
import com.foundationdb.server.entity.model.EntityVisitor;
import com.foundationdb.server.entity.model.FieldProperty;
import com.foundationdb.server.entity.model.IndexField;
import com.foundationdb.server.entity.model.EntityIndex;
import com.foundationdb.server.entity.model.Validation;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TInstance;
import com.foundationdb.server.types3.texpressions.Serialization;
import com.google.common.base.Function;
import com.google.common.collect.BiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class EntityToAIS implements EntityVisitor {
    private static final Logger LOG = LoggerFactory.getLogger(EntityToAIS.class);

    private static final boolean ATTR_REQUIRED_DEFAULT = false;
    private static final Index.JoinType GI_JOIN_TYPE_DEFAULT = Index.JoinType.LEFT;

    private final String schemaName;
    private final AISBuilder builder = new AISBuilder();
    private final Deque<TableInfo> tableInfoStack = new ArrayDeque<>();
    private TableName groupName = null;

    private static final Function<String, TypeInfo> typeNameResolver = createTypeNameResolver();

    private static final class TypeInfo {

        private TypeInfo(Type type, TClass tClass) {
            this.type = type;
            this.tClass = tClass;
        }

        private final Type type;
        private final TClass tClass;
    }

    public EntityToAIS(String schemaName) {
        this.schemaName = schemaName;
    }

    //
    // EntityVisitor
    //

    @Override
    public void enterTopEntity(Entity entity) {
        String name = entity.getName();
        builder.createGroup(name, schemaName);
        groupName = new TableName(schemaName, name);
        startTable(entity);
        builder.addTableToGroup(groupName, schemaName, name);
        finishTable(entity);
    }

    @Override
    public void leaveTopEntity() {
        tableInfoStack.pop();
        assert tableInfoStack.isEmpty() : tableInfoStack;
        builder.basicSchemaIsComplete();
        builder.groupingIsComplete();
        groupName = null;
    }

    private void visitField(Entity entity, int fieldIndex) {
        EntityField field = entity.getFields().get(fieldIndex);
        String name = field.getName();
        TypeInfo scalarType = typeNameResolver.apply(field.getType());
        assert scalarType != null : name;
        ColumnInfo info = getColumnInfo(scalarType,
                                        field.getProperties(),
                                        field.getValidations());
        if (entity.getIdentifying().contains(name))
            info.nullable = false;
        Column column = builder.column(schemaName, entity.getName(),
                                       name, fieldIndex,
                                       scalarType.type.name(), info.param1, info.param2,
                                       info.nullable, false /*isAutoInc*/,
                                       info.charset, info.collation);
        if (info.identity != null)
            builder.columnAsIdentity(schemaName, entity.getName(), name,
                                     info.identity.getStart(), info.identity.getIncrement(), info.identity.isDefault());
        column.setUuid(field.getUuid());
    }

    @Override
    public void enterCollections() {
        // Nothing
    }

    @Override
    public void enterCollection(EntityCollection collection) {
        final TableInfo parentInfo = tableInfoStack.peek();
        final UserTable childTable = startTable(collection);
        // FKs
        buildFks(collection, childTable, parentInfo);

        // Done
        finishTable(collection);
    }

    private void buildFks(EntityCollection childEntity, UserTable childTable, TableInfo parentInfo) {
        final Entity parentEntity = parentInfo.entity;
        UserTable parentTable = parentInfo.table;
        List<String> pkFields = parentEntity.getIdentifying();
        if (pkFields.isEmpty())
            throw new IllegalArgumentException(parentTable + " has no PK, but has child " + childEntity);
        final List<String> fkFields = childEntity.getGroupingFields();
        final boolean generateFkCols = fkFields.isEmpty();
        if ((!generateFkCols) && (childEntity.getGroupingFields().size() != parentEntity.getIdentifying().size()))
            throw new IllegalArgumentException("grouping fields don't match: " + childEntity);

        String parentName = parentEntity.getName();
        String joinName = childEntity.getName() + "_" + parentName;
        builder.joinTables(joinName, schemaName, parentName, schemaName, childEntity.getName());

        for (int i = 0, len = pkFields.size(), offset = childTable.getColumns().size(); i < len; ++i) {
            String parentColName = pkFields.get(i);
            Column parentCol = parentTable.getColumn(parentColName);
            String childColName;
            if (generateFkCols) {
                childColName = createColumnName(childTable.getColumns(), parentColName + "_ref");
                Column newCol = Column.create(childTable, parentCol, childColName, i + offset);
                // Should be exactly the same *except* UUID
                newCol.setUuid(null);
            }
            else {
                childColName = fkFields.get(i);
            }
            builder.joinColumns(joinName,
                    schemaName, parentName, parentColName,
                    schemaName, childEntity.getName(), childColName);
            builder.addJoinToGroup(groupName, joinName, 0);
        }
    }

    @Override
    public void leaveCollection() {
        tableInfoStack.pop();
    }

    @Override
    public void leaveCollections() {
    }

    private Set<String> getUniqueIndexes(Set<Validation> validations) {
        Set<String> uniques = new HashSet<>(validations.size());
        for(Validation v : validations) {
            if("unique".equals(v.getName())) {
                String indexName = (String)v.getValue();
                if (!uniques.add(indexName))
                    LOG.warn("Duplicate UNIQUE constraint on index: {}", indexName);
            } else {
                LOG.warn("Ignored entity validation on {}: {}", groupName, v);
            }
        }
        return uniques;
    }

    private void visitIndexes(BiMap<String, EntityIndex> indexes, Entity context, Set<String> uniques) {
        for(Map.Entry<String,EntityIndex> entry : indexes.entrySet()) {
            String indexName = entry.getKey();
            List<IndexField> columns = entry.getValue().getFields();
            boolean isGI = isMultiTable(columns, context);
            boolean isUnique = uniques.remove(indexName);
            if(isGI) {
                if(isUnique) {
                    throw new IllegalArgumentException("Unique group index not allowed");
                }
                builder.groupIndex(groupName, indexName, false, GI_JOIN_TYPE_DEFAULT);
                int pos = 0;
                for(IndexField col : columns) {
                    TableName colName = getFieldName(col, context);
                    if (!isCurrentOrAncestor(colName.getSchemaName()))
                        throw new IllegalArgumentException(indexName + ": "
                                + colName + " belongs to a table that isn't an ancestor of " + context);
                    builder.groupIndexColumn(groupName, indexName, schemaName,
                            colName.getSchemaName(), colName.getTableName(), pos++);
                }
            } else {
                builder.index(schemaName, context.getName(), indexName, isUnique, Index.KEY_CONSTRAINT);
                int pos = 0;
                for(IndexField col : columns) {
                    TableName colName = getFieldName(col, context);
                    builder.indexColumn(schemaName, colName.getSchemaName(), indexName,
                            colName.getTableName(), pos++, true, null);
                }
            }
        }
        if (!uniques.isEmpty())
            throw new IllegalArgumentException("UNIQUE constraint for undefined index(es) on " + context
                    + ": " + uniques);
    }

    private boolean isCurrentOrAncestor(String tableName) {
        for (TableInfo table : tableInfoStack) {
            if (table.entity.getName().equals(tableName))
                return true;
        }
        return false;
    }

    private TableName getFieldName(IndexField indexField, Entity context) {
        TableName theColName;
        if (indexField instanceof IndexField.QualifiedFieldName) {
            IndexField.QualifiedFieldName qualified = (IndexField.QualifiedFieldName) indexField;
            theColName = new TableName(qualified.getEntityName(), qualified.getFieldName());
        }
        else if (indexField instanceof IndexField.FieldName) {
            IndexField.FieldName fieldName = (IndexField.FieldName) indexField;
            theColName = new TableName(context.getName(), fieldName.getFieldName());
        }
        else throw new AssertionError(indexField);
        return theColName;
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

    private UserTable startTable(Entity entity) {
        String name = entity.getName();
        UserTable table = builder.userTable(schemaName, name);
        table.setUuid(entity.getUuid());
        // fields
        for (int f = 0, len = entity.getFields().size(); f < len; f++)
            visitField(entity, f);
        // PK
        List<String> pkFields = entity.getIdentifying();
        if (!pkFields.isEmpty()) {
            builder.index(schemaName, name, Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
            int pos = 0;
            for(String column : pkFields)
                builder.indexColumn(schemaName, name, Index.PRIMARY_KEY_CONSTRAINT, column, pos++, true, null);
        }

        // done
        tableInfoStack.push(new TableInfo(table, entity));
        return table;
    }

    private void finishTable(Entity entity) {
        // secondary indexes
        Set<String> uniques = getUniqueIndexes(entity.getValidations());
        visitIndexes(entity.getIndexes(), entity, uniques);
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

    private static ColumnInfo getColumnInfo(TypeInfo type, Map<String,Object> props, Collection<Validation> validations)
    {
        ColumnInfo info = new ColumnInfo();

        Map<String,Object> fullProps = new HashMap<>();
        for (Map.Entry<String, Object> entry : props.entrySet())
            fullProps.put(entry.getKey().toUpperCase(), entry.getValue());

        for(Validation v : validations) {
            if("required".equals(v.getName())) {
                boolean isRequired = (Boolean)v.getValue();
                info.nullable = !isRequired;
            }
            else {
                fullProps.put(v.getName().toUpperCase(), v.getValue());
            }
        }

        for (Map.Entry<? extends com.foundationdb.server.types3.Attribute, ? extends Serialization> t3Attr
                : type.tClass.attributeSerializations().entrySet())
        {
            Serialization serialization = t3Attr.getValue();
            if (serialization != null) {
                switch (serialization) {
                case CHARSET:
                    info.charset = maybeString(fullProps, t3Attr.getKey());
                    break;
                case COLLATION:
                    info.collation = maybeString(fullProps, t3Attr.getKey());
                    break;
                case LONG_1:
                    info.param1 = maybeLong(fullProps, t3Attr.getKey());
                    break;
                case LONG_2:
                    info.param2 = maybeLong(fullProps, t3Attr.getKey());
                    break;
                default:
                    throw new AssertionError(serialization + " for attribute " + t3Attr);
                }
            }
        }
        Object identity = fullProps.remove(FieldProperty.IdentityProperty.PROPERTY_NAME.toUpperCase());
        if(identity != null) {
            info.identity = FieldProperty.IdentityProperty.create(identity);
        }
        if (!fullProps.isEmpty()) {
            LOG.warn("unused properties or validations for column of type {}: {}", type.tClass, fullProps);
        }
        return info;
    }

    private static boolean isMultiTable(List<IndexField> columns, Entity context) {
        for (IndexField field : columns) {
            if (field instanceof IndexField.QualifiedFieldName) {
                IndexField.QualifiedFieldName qualified = (IndexField.QualifiedFieldName) field;
                String fieldEntity = qualified.getEntityName();
                if (!fieldEntity.equals(context.getName()))
                    return true;
            }
        }
        return false;
    }

    private static Long maybeLong(Map<String, Object> props, com.foundationdb.server.types3.Attribute attribute) {
        Object o = null;
        if (props.containsKey(attribute.name().toUpperCase())) {
            o = props.remove(attribute.name());
        }
        if (o == null)
            return null;
        else if (o.getClass() == Long.class)
            return (Long) o;
        else
            return ((Number)o).longValue();
    }

    private static String maybeString(Map<String, Object> props, com.foundationdb.server.types3.Attribute attribute) {
        Object o = null;
        if (props.containsKey(attribute.name().toUpperCase()))
            o = props.remove(attribute.name());
        return (o != null) ? o.toString() : null;
    }

    private static class TableInfo {
        public final UserTable table;
        public final Entity entity;

        private TableInfo(UserTable table, Entity entity) {
            this.table = table;
            this.entity = entity;
        }

        @Override
        public String toString() {
            return entity.toString();
        }
    }

    private static Function<String, TypeInfo> createTypeNameResolver() {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        Collection<Type> aisTypes = ais.getTypes();
        final Map<String, TypeInfo> types = new HashMap<>(aisTypes.size());
        CharsetAndCollation dummyCharset = ais.getCharsetAndCollation();
        Set<Type> unsupportedTypes = Types.unsupportedTypes();
        for (Type type : aisTypes) {
            if (unsupportedTypes.contains(type))
                continue;
            // We create a dummy instance using values we don't care about, but which will be valid for all types.
            // All we need from it is the TClass's name
            TInstance dummyInstance = Column.generateTInstance(dummyCharset, type, 3L, 3L, true);
            TClass tClass = dummyInstance.typeClass();
            String typeName = tClass.name().unqualifiedName();
            if (null != types.put(typeName.toLowerCase(), new TypeInfo(type, tClass)))
                throw new RuntimeException("can't compute (name -> Type) map because of conflict: " + typeName);
        }
        return new Function<String, TypeInfo>() {
            @Override
            public TypeInfo apply(String input) {
                TypeInfo type = types.get(input.toLowerCase());
                if (type == null)
                    throw new NoSuchElementException(input);
                return type;
            }
        };
    }

    private static class ColumnInfo {
        public Long param1;
        public Long param2;
        public String charset;
        public String collation;
        public boolean nullable = !ATTR_REQUIRED_DEFAULT;
        public FieldProperty.IdentityProperty identity;
    }
}
