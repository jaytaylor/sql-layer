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

import com.akiban.server.entity.model.AbstractEntityVisitor;
import com.akiban.server.entity.model.Attribute;
import com.akiban.server.entity.model.Entity;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.UUID;

public final class AttributeLookups {

    public Map<UUID, Attribute> getAttributesByUuid() {
        return attributesByUuid;
    }

    public List<UUID> pathFor(UUID uuid) {
        return pathsByUuid.get(uuid);
    }

    public List<String> pathNamesFor(UUID uuid) {
        return Lists.transform(pathFor(uuid), new Function<UUID, String>() {
            @Override
            public String apply(UUID pathSegment) {
                return nameFor(pathSegment);
            }
        });
    }

    public Attribute attributeFor(UUID uuid) {
        return attributesByUuid.get(uuid);
    }

    public String nameFor(UUID uuid) {
        return namesByUuid.get(uuid);
    }

    public boolean containsAttribute(UUID uuid) {
        return attributesByUuid.containsKey(uuid);
    }

    public AttributeLookups(Entity entity) {
        entity.accept(null, new Visitor());
    }

    public UUID getParentAttribute(UUID uuid) {
        List<UUID> path = pathsByUuid.get(uuid);
        if (path == null)
            throw new NoSuchElementException(String.valueOf(uuid));
        return path.size() < 2 ? null : path.get(0);

    }

    private Map<UUID, Attribute> attributesByUuid = new HashMap<>();
    private Map<UUID, List<UUID>> pathsByUuid = new HashMap<>();
    private Map<UUID, String> namesByUuid = new HashMap<>();

    private class Visitor extends AbstractEntityVisitor {
        @Override
        public void visitEntity(String name, Entity entity) {
            currentPath.push(entity.uuid());
        }

        @Override
        public void leaveEntity() {
            currentPath.pop();
        }

        @Override
        public void visitCollection(String name, Attribute collection) {
            seeAttribute(name, collection);
            currentPath.push(collection.getUUID());
        }

        @Override
        public void leaveCollection() {
            currentPath.pop();
        }

        @Override
        public void visitScalar(String name, Attribute scalar) {
            seeAttribute(name, scalar);
        }

        private void seeAttribute(String name, Attribute attribute) {
            attributesByUuid.put(attribute.getUUID(), attribute);
            pathsByUuid.put(attribute.getUUID(), ImmutableList.copyOf(currentPath));
            namesByUuid.put(attribute.getUUID(), name);
        }

        private Deque<UUID> currentPath = new ArrayDeque<>();
    }
}
