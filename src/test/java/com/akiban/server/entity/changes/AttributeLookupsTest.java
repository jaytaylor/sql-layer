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
import org.junit.Test;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.UUID;

public final class AttributeLookupsTest {
    @Test
    public void testGetParentAttribute() throws Exception {
        Space space = Space.readSpace("attribute_lookups_space.json", AttributeLookupsTest.class, null);
        Entity customer = space.getEntities().get("customer");
        AttributeLookups lookups = new AttributeLookups(customer);

        Map<String, String> parents = new TreeMap<>();
        for (UUID uuid : lookups.getAttributesByUuid().keySet()) {
            UUID parent = lookups.getParentAttribute(uuid);
            parents.put(uuid.toString(), String.valueOf(parent));
        }

        Map<String, String> expectedParents = new TreeMap<>();
        expectedParents.put("257e9b59-e77f-4a4d-a5da-00c7c2261875", "null");
        expectedParents.put("c5cedd91-9751-41c2-9417-8c29117ca2c9", "null");
        expectedParents.put("58dd53b7-e8a1-488b-a751-c83f9beca04c", "c5cedd91-9751-41c2-9417-8c29117ca2c9");
        expectedParents.put("378d22b2-acf0-45d3-8f8a-2c09188a1cf3", "c5cedd91-9751-41c2-9417-8c29117ca2c9");
        expectedParents.put("24b0cf4f-6a33-43c4-bd15-db3a7b06d963", "378d22b2-acf0-45d3-8f8a-2c09188a1cf3");

        JUnitUtils.equalMaps("parents", expectedParents, parents);
    }

    @Test(expected = NoSuchElementException.class)
    public void testGetParentAttributeOfUnknown() {
        Space space = Space.readSpace("attribute_lookups_space.json", AttributeLookupsTest.class, null);
        Entity customer = space.getEntities().get("customer");
        AttributeLookups lookups = new AttributeLookups(customer);
        lookups.getParentAttribute(customer.uuid());

    }
}
