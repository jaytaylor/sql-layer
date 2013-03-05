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
