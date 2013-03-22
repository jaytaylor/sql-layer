
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
