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
import com.akiban.server.entity.model.Space;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;

public final class EntityElementLookups {

    public List<EntityElement> ancestorsFor(UUID uuid) {
        return pathsByUuid.get(uuid);
    }

    public Map<UUID, EntityElement> getElementsByUuid() {
        return elementsByUuid;
    }

    public EntityElement getParent(UUID uuid) {
        List<EntityElement> path = pathsByUuid.get(uuid);
        if (path == null)
            throw new NoSuchElementException(String.valueOf(uuid));
        return path.size() < 1 ? null : path.get(0);
    }

    Set<UUID> getUuids() {
        return elementsByUuid.keySet();
    }

    public EntityElementLookups(Space space) {
        Visitor visitor = new Visitor();
        for (Entity entity : space.getEntities())
            entity.accept(visitor);
        elementsByUuid  = Collections.unmodifiableMap(visitor.elementsByUuid);
        pathsByUuid = Collections.unmodifiableMap(visitor.pathsByUuid);
    }

    private final Map<UUID, EntityElement> elementsByUuid;
    private final Map<UUID, List<EntityElement>> pathsByUuid;

    private static class Visitor extends AbstractEntityVisitor {

        @Override
        public void visitEntity(Entity entity) {
            currentPath.push(entity);
        }

        @Override
        protected void leaveEntity() {
            currentPath.pop();
        }

        @Override
        protected void visitEntityElement(EntityElement element) {
            pathsByUuid.put(element.getUuid(), ImmutableList.copyOf(currentPath));
            Object old = elementsByUuid.put(element.getUuid(), element);
            if (old != null)
                throw new IllegalStateException("uuid already seen: " + element.getUuid());
        }

        private final Deque<EntityElement> currentPath = new ArrayDeque<>();
        private final Map<UUID, EntityElement> elementsByUuid = new HashMap<>();
        private final Map<UUID, List<EntityElement>> pathsByUuid = new HashMap<>();
    }
}
