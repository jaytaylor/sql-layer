/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.entity.fromais;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.PrimaryKey;
import com.akiban.ais.model.UserTable;
import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.EntityCollection;
import com.akiban.server.entity.model.IndexField;
import com.akiban.server.entity.model.EntityField;
import com.akiban.server.entity.model.EntityIndex;
import com.akiban.server.entity.model.Validation;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.google.common.base.Predicate;
import com.google.common.collect.BiMap;
import com.google.common.collect.Collections2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class EntityBuilder {

    public EntityBuilder(UserTable rootTable) {
        entity = Entity.modifiableEntity(uuidOrCreate(rootTable));
        buildContainer(entity, rootTable);
        Set<String> uniques = buildIndexes(entity.getIndexes());
        buildUniques(entity.getValidations(), uniques);
    }

    private void buildScalars(List<EntityField> fields, UserTable table) {
        for (Column column : table.getColumns()) {
            TInstance tInstance = column.tInstance();
            TClass tClass = tInstance.typeClass();
            String type = tClass.name().unqualifiedName().toLowerCase();
            EntityField scalar = EntityField.modifiableScalar(uuidOrCreate(column), type);
            scalar.setName(column.getName());

            Map<String, Object> properties = scalar.getProperties();
            Collection<Validation> validations = scalar.getValidations();
            validations.add(new Validation("required", !tInstance.nullability()));
            for (com.akiban.server.types3.Attribute t3Attr : tClass.attributes()) {
                String attrName = t3Attr.name().toLowerCase();
                Object attrValue = tInstance.attributeToObject(t3Attr);
                if (tClass.attributeIsPhysical(t3Attr))
                    properties.put(attrName, attrValue);
                else
                    validations.add(new Validation(attrName, attrValue));
            }
            fields.add(scalar);
        }
    }

    private void buildIdentifying(List<String> identifying, UserTable rootTable) {
        PrimaryKey pk = rootTable.getPrimaryKey();
        if (pk != null) {
            for (Column c : pk.getColumns())
                identifying.add(c.getName());
        }
    }

    private void buildCollections(Collection<EntityCollection> collections, UserTable table) {
        List<Join> childJoins = table.getChildJoins();
        for (Join childJoin : childJoins) {
            UserTable child = childJoin.getChild();
            collections.add(buildCollection(child));
        }
        // while we're here...
        indexes.addAll(Collections2.filter(table.getIndexes(), nonPks));
        for (GroupIndex gi : table.getGroupIndexes()) {
            if (gi.getJoinType() != Index.JoinType.LEFT)
                throw new InconvertibleAisException("can't convert non-LEFT group indexes: " + gi);
            if (gi.leafMostTable() == table)
                indexes.add(gi);
        }
    }

    private EntityCollection buildCollection(UserTable table) {
        EntityCollection collection = EntityCollection.modifiableCollection(uuidOrCreate(table));
        buildContainer(collection, table);
        return collection;
    }

    private void buildContainer(Entity container, UserTable table) {
        buildScalars(container.getFields(), table);
        buildIdentifying(container.getIdentifying(), table);
        buildCollections(container.getCollections(), table);
        container.setName(table.getName().getTableName());
    }

    /**
     * Build the indexes, and return back the names of the unique ones. This assumes that indexes have already
     * been compiled into the "indexes" collection.
     * @param out the map to insert into
     * @return the json names of the unique indexes
     */
    private Set<String> buildIndexes(BiMap<String, EntityIndex> out) {
        Set<String> uniques = new HashSet<>(out.size());
        for (Index index : indexes) {
            if (index.getIndexName().getName().startsWith("__akiban"))
                continue;
            String jsonName = index.getIndexName().getName();
            List<IndexColumn> keyColumns = index.getKeyColumns();
            List<IndexField> indexFields = new ArrayList<>(keyColumns.size());
            for (IndexColumn indexCol : keyColumns) {
                Column col = indexCol.getColumn();
                IndexField indexField = new IndexField.QualifiedFieldName(
                        col.getTable().getName().getTableName(),
                        col.getName());
                indexFields.add(indexField);
            }
            EntityIndex entityIndex = new EntityIndex();
            entityIndex.setFields(indexFields);
            EntityIndex old = out.put(jsonName, entityIndex);
            if (old != null)
                throw new InconvertibleAisException("duplicate index name: " + jsonName);
            if (index.isUnique())
                uniques.add(jsonName);
        }
        return uniques;
    }

    private void buildUniques(Collection<Validation> validation, Set<String> uniques) {
        for (String uniqueIndex : uniques) {
            validation.add(new Validation("unique", uniqueIndex));
        }
    }

    public Entity getEntity() {
        return entity;
    }

    private static UUID uuidOrCreate(UserTable table) {
        UUID uuid = table.getUuid();
        assert uuid != null : table;
        return uuid;
    }

    private UUID uuidOrCreate(Column column) {
        UUID uuid = column.getUuid();
        assert uuid != null : column;
        return uuid;
    }

    private final Entity entity;
    private final List<Index> indexes = new ArrayList<>();

    private static final Predicate<? super Index> nonPks = new Predicate<Index>() {
        @Override
        public boolean apply(Index input) {
            return ! input.isPrimaryKey();
        }
    };
}
