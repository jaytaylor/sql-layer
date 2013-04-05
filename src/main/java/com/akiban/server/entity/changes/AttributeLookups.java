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
import com.akiban.server.entity.model.EntityElement;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

public final class AttributeLookups {

    public List<UUID> pathFor(UUID uuid) {
        return pathsByUuid.get(uuid);
    }

    public EntityElement getElement(UUID uuid) {
        return elementsByUuid.get(uuid);
    }

    public AttributeLookups(Entity entity) {
        Visitor visitor = new Visitor();
        entity.accept(visitor);
        elementsByUuid  = Collections.unmodifiableMap(visitor.elementsByUuid);
        pathsByUuid = Collections.unmodifiableMap(visitor.pathsByUuid);
    }

    public UUID getParent(UUID uuid) {
        List<UUID> path = pathsByUuid.get(uuid);
        if (path == null)
            throw new NoSuchElementException(String.valueOf(uuid));
        return path.size() < 1 ? null : path.get(0);
    }

    Set<UUID> getUuids() {
        return elementsByUuid.keySet();
    }

    private final Map<UUID, EntityElement> elementsByUuid;
    private final Map<UUID, List<UUID>> pathsByUuid;

    private static class Visitor extends AbstractEntityVisitor {

        @Override
        public void visitEntity(Entity entity) {
            currentPath.push(entity.getUuid());
        }

        @Override
        protected void visitEntityElement(EntityElement element) {
            pathsByUuid.put(element.getUuid(), ImmutableList.copyOf(currentPath));
            elementsByUuid.put(element.getUuid(), element);
        }

        private final Deque<UUID> currentPath = new ArrayDeque<>();
        private final Map<UUID, EntityElement> elementsByUuid = new HashMap<>();
        private final Map<UUID, List<UUID>> pathsByUuid = new HashMap<>();
    }
}
