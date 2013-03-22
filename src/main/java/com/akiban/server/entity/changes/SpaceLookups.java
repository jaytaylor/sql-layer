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

import com.akiban.server.entity.model.Entity;
import com.akiban.server.entity.model.Space;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

class SpaceLookups {

    public boolean containsUuid(UUID uuid) {
        return entriesByUuid.containsKey(uuid);
    }

    public Map<UUID, Entity> entitiesByUuid() {
        return entriesByUuid;
    }

    public Set<UUID> keySet() {
        return entriesByUuid.keySet();
    }

    public String getName(UUID uuid) {
        return namesByUuid.get(uuid);
    }

    public Entity getEntity(UUID uuid) {
        return entriesByUuid.get(uuid);
    }

    public SpaceLookups(Space space) {
        Map<String, Entity> entities = space.getEntities();
        for (Map.Entry<String, Entity> entry : entities.entrySet()) {
            String name = entry.getKey();
            Entity entity = entry.getValue();
            entriesByUuid.put(entity.uuid(), entity);
            namesByUuid.put(entity.uuid(), name);
        }
    }

    private Map<UUID, Entity> entriesByUuid = new HashMap<>();
    private Map<UUID, String> namesByUuid = new HashMap<>();
}
