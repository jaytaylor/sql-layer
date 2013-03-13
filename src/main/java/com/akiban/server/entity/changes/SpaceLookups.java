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
