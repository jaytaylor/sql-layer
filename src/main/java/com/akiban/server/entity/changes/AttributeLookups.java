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

package com.akiban.server.entity.changes;

import com.akiban.server.entity.model.AbstractEntityVisitor;
import com.akiban.server.entity.model.Entity;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

public final class AttributeLookups {

//    public Map<UUID, Attribute> getAttributesByUuid() {
//        return attributesByUuid;
//    }

    public List<UUID> pathFor(UUID uuid) {
        return pathsByUuid.get(uuid);
    }

//    public Attribute attributeFor(UUID uuid) {
//        return attributesByUuid.get(uuid);
//    }

    public String nameFor(UUID uuid) {
        return namesByUuid.get(uuid);
    }

//    public boolean containsAttribute(UUID uuid) {
//        return attributesByUuid.containsKey(uuid);
//    }

//    public AttributeLookups(Entity entity) {
//        entity.accept(null, new Visitor());
//    }

    public UUID getParentAttribute(UUID uuid) {
        List<UUID> path = pathsByUuid.get(uuid);
        if (path == null)
            throw new NoSuchElementException(String.valueOf(uuid));
        return path.size() < 2 ? null : path.get(0);

    }

//    private Map<UUID, Attribute> attributesByUuid = new HashMap<>();
    private Map<UUID, List<UUID>> pathsByUuid = new HashMap<>();
    private Map<UUID, String> namesByUuid = new HashMap<>();

    private class Visitor extends AbstractEntityVisitor {
//        @Override
//        public void visitEntity(String name, Entity entity) {
//            currentPath.push(entity.uuid());
//        }
//
//        @Override
//        public void leaveTopEntity() {
//            currentPath.pop();
//        }
//
//        @Override
//        public void visitCollection(String name, Attribute collection) {
//            seeAttribute(name, collection);
//            currentPath.push(collection.getUUID());
//        }
//
//        @Override
//        public void leaveCollection() {
//            currentPath.pop();
//        }
//
//        @Override
//        public void visitScalar(String name, Attribute scalar) {
//            seeAttribute(name, scalar);
//        }
//
//        private void seeAttribute(String name, Attribute attribute) {
//            attributesByUuid.put(attribute.getUUID(), attribute);
//            pathsByUuid.put(attribute.getUUID(), ImmutableList.copyOf(currentPath));
//            namesByUuid.put(attribute.getUUID(), name);
//        }
//
//        private Deque<UUID> currentPath = new ArrayDeque<>();
    }
}
