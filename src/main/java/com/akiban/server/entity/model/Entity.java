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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public final class Entity extends EntityContainer {

    public Collection<Validation> getValidations() {
        return validations;
    }

    void setValidations(Collection<Validation> validations) {
        Set<Validation> validationsSet = new TreeSet<>(validations);
        if (validationsSet.size() != validations.size())
            throw new IllegalEntityDefinition("duplicate validations given: " + validations);
        this.validations = validationsSet;
    }

    public BiMap<String, EntityIndex> getIndexes() {
        return indexes;
    }

    @SuppressWarnings("unused")
    void setIndexes(Map<String, EntityIndex> indexes) {
        this.indexes = HashBiMap.create(indexes.size());
        for (Map.Entry<String, EntityIndex> entry : indexes.entrySet()) {
            if (this.indexes.put(entry.getKey(), entry.getValue()) != null)
                throw new IllegalEntityDefinition("multiple names given for index: " + entry);
        }
        this.indexes = ImmutableBiMap.copyOf(this.indexes);
    }

    @Override
    public String toString() {
        return String.format("entity {%s}", getUuid());
    }

    private Set<Validation> validations = Collections.emptySet();
    private BiMap<String, EntityIndex> indexes = ImmutableBiMap.of();

    public static Entity modifiableEntity(UUID uuid) {
        Entity entity = new Entity();
        entity.makeModifiable(uuid);
        entity.validations = new TreeSet<>();
        entity.indexes = HashBiMap.create();
        return entity;
    }

    Entity() {}

    public <E extends Exception> void accept(EntityVisitor<E>  visitor) throws E {
        visitor.visitEntity(this);
        for (EntityCollection collection : getCollections()) {
            collection.accept(visitor);
        }
        visitor.leaveEntityAttributes();
        visitor.visitEntityValidations(validations);
        visitor.visitIndexes(indexes);
        visitor.leaveEntity();
    }
}
