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

package com.akiban.server.entity.model;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

public final class Entity {
    public UUID uuid() {
        return uuid;
    }

    void setUuid(UUID uuid) {
        if (this.uuid != null)
            throw new IllegalStateException("uuid already set: " + this.uuid);
        this.uuid = uuid;
    }


    @SuppressWarnings("unused")
    void setEntity(String entity) {
        this.uuid = Util.parseUUID(entity);
    }

    public Map<String, Attribute> getAttributes() {
        return attributes;
    }

    @SuppressWarnings("unused")
    void setAttributes(Map<String, Attribute> attributes) {
        this.attributes = Collections.unmodifiableMap(new TreeMap<>(attributes));
    }

    public Collection<Validation> getValidation() {
        return validations;
    }

    @SuppressWarnings("unused")
    void setValidation(List<Map<String, ?>> validations) {
        this.validations = Validation.createValidations(validations);
    }

    public BiMap<String, EntityIndex> getIndexes() {
        return indexes;
    }

    @SuppressWarnings("unused")
    void setIndexes(Map<String, List<List<String>>> indexes) {
        this.indexes = HashBiMap.create(indexes.size());
        for (Map.Entry<String, List<List<String>>> entry : indexes.entrySet()) {
            List<EntityColumn> columns = Lists.transform(entry.getValue(), EntityColumn.namesToColumn);
            EntityIndex index = new EntityIndex(columns);
            if (this.indexes.put(entry.getKey(), index) != null)
                throw new IllegalEntityDefinition("multiple names given for index: " + index);
        }
        this.indexes = ImmutableBiMap.copyOf(this.indexes);
    }

    @Override
    public String toString() {
        return String.format("entity {%s}", uuid());
    }

    private UUID uuid;
    private Map<String, Attribute> attributes = Collections.emptyMap();
    private Set<Validation> validations = Collections.emptySet();
    private BiMap<String, EntityIndex> indexes = ImmutableBiMap.of();

    public static Entity modifiableEntity(UUID uuid) {
        Entity entity = new Entity();
        entity.uuid = uuid;
        entity.attributes = new TreeMap<>();
        entity.validations = new TreeSet<>();
        entity.indexes = HashBiMap.create();
        return entity;
    }

    Entity() {}

    public <E extends Exception> void accept(String myName, EntityVisitor<E>  visitor) throws E {
        visitor.visitEntity(myName, this);
        for (Map.Entry<String, Attribute> entry : attributes.entrySet()) {
            entry.getValue().accept(entry.getKey(), visitor);
        }
        visitor.leaveEntityAttributes();
        visitor.visitEntityValidations(validations);
        visitor.visitIndexes(indexes);
        visitor.leaveEntity();
    }
}
