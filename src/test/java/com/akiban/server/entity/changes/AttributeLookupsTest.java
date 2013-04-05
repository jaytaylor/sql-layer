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
import com.akiban.util.JUnitUtils;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.UUID;

public final class AttributeLookupsTest {
    @Test
    public void testAncestors() throws Exception {
        Space space = Space.readSpace("attribute_lookups_space.json", AttributeLookupsTest.class, null);
        Entity customer = getEntity(space, "customer");
        AttributeLookups lookups = new AttributeLookups(customer);

        Paths actual = new Paths();
        for (UUID uuid : lookups.getUuids())
            actual.put(uuid, lookups.pathFor(uuid));

        Paths expected = new Paths()
                .put("2a57b59e-e1b7-4377-996e-a5c04e0abf29")
                .put("257e9b59-e77f-4a4d-a5da-00c7c2261875", "2a57b59e-e1b7-4377-996e-a5c04e0abf29")
                .put("c5cedd91-9751-41c2-9417-8c29117ca2c9", "2a57b59e-e1b7-4377-996e-a5c04e0abf29")
                .put("58dd53b7-e8a1-488b-a751-c83f9beca04c", "c5cedd91-9751-41c2-9417-8c29117ca2c9", "2a57b59e-e1b7-4377-996e-a5c04e0abf29")
                .put("378d22b2-acf0-45d3-8f8a-2c09188a1cf3", "c5cedd91-9751-41c2-9417-8c29117ca2c9", "2a57b59e-e1b7-4377-996e-a5c04e0abf29")
                .put("24b0cf4f-6a33-43c4-bd15-db3a7b06d963", "378d22b2-acf0-45d3-8f8a-2c09188a1cf3", "c5cedd91-9751-41c2-9417-8c29117ca2c9", "2a57b59e-e1b7-4377-996e-a5c04e0abf29");

        JUnitUtils.equalMaps("paths", expected.paths, actual.paths);
    }

    @Test
    public void testParents() throws Exception {
        Space space = Space.readSpace("attribute_lookups_space.json", AttributeLookupsTest.class, null);
        Entity customer = getEntity(space, "customer");
        AttributeLookups lookups = new AttributeLookups(customer);

        Map<String, String> actual = new TreeMap<>();
        for (UUID uuid : lookups.getUuids()) {
            UUID parent = lookups.getParent(uuid);
            String parentStr = parent == null ? null : parent.toString();
            actual.put(uuid.toString(), parentStr);
        }

        Map<String, String> expected = new TreeMap<>();
        expected.put("2a57b59e-e1b7-4377-996e-a5c04e0abf29", null);
        expected.put("257e9b59-e77f-4a4d-a5da-00c7c2261875", "2a57b59e-e1b7-4377-996e-a5c04e0abf29");
        expected.put("c5cedd91-9751-41c2-9417-8c29117ca2c9", "2a57b59e-e1b7-4377-996e-a5c04e0abf29");
        expected.put("58dd53b7-e8a1-488b-a751-c83f9beca04c", "c5cedd91-9751-41c2-9417-8c29117ca2c9");
        expected.put("378d22b2-acf0-45d3-8f8a-2c09188a1cf3", "c5cedd91-9751-41c2-9417-8c29117ca2c9");
        expected.put("24b0cf4f-6a33-43c4-bd15-db3a7b06d963", "378d22b2-acf0-45d3-8f8a-2c09188a1cf3");

        JUnitUtils.equalMaps("paths", expected, actual);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetParentAttributeOfUnknown() {
        Space space = Space.readSpace("attribute_lookups_space.json", AttributeLookupsTest.class, null);
        Entity customer = getEntity(space, "customer");
        AttributeLookups lookups = new AttributeLookups(customer);
        UUID notThere = UUID.fromString("941b9155-d1a0-4166-a369-3e8c3ce3c53b");
        lookups.getParent(notThere);
    }

    private static Entity getEntity(Space space, String name) {
        for (Entity entity : space.getEntities()) {
            if (name.equals(entity.getName()))
                return entity;
        }
        throw new NoSuchElementException(name);
    }

    private static class Paths {
        Map<String, List<String>> paths = new TreeMap<>();

        public Paths put(Object uuid, Object... ancestors) {
            return put(uuid, Arrays.asList(ancestors));
        }

        public Paths put(Object uuid, List<?> ancestors) {
            Object old = paths.put(uuid.toString(), Lists.transform(ancestors, Functions.toStringFunction()));
            assert old == null : uuid;
            return this;
        }
    }
}
