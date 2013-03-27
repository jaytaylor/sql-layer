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

package com.akiban.server.entity.model;

import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.akiban.util.AssertUtils.assertCollectionEquals;
import static com.akiban.util.JUnitUtils.equalsIncludingHash;
import static com.akiban.util.JUnitUtils.isUnmodifiable;
import static com.akiban.util.JUnitUtils.map;
import static com.google.common.collect.Sets.newHashSet;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class SpaceTest {
    @Test()
    public void coi() {
        // space
        Space space = Space.readSpace("coi.json", SpaceTest.class, null);
        isUnmodifiable("space", space.getEntities());
        assertEquals("space keys", set("customer"), space.getEntities());

        Entity customer = space.getEntities().get("customer");
        assertEquals("customer uuid", UUID.fromString("2a57b59e-e1b7-4377-996e-a5c04e0abf29"), customer.uuid());
        isUnmodifiable("customer attributes", customer.getAttributes());
        assertEquals("customer attributes", set("id", "last_name", "orders"), customer.getAttributes().keySet());

        isUnmodifiable("customer indexes", customer.getIndexes());
        assertEquals("customer index names",
                set("orderplaced_lastname"),
                set(customer.getIndexes().keySet()));
        EntityIndex index = customer.getIndexes().get("orderplaced_lastname");
        List<IndexField> expectedColumns = asList(
                new IndexField("orders", "placed"), new IndexField("customer", "last_name")
        );
        isUnmodifiable("index columns", index.getFields());
        assertEquals("index columns", expectedColumns, index.getFields());
        equalsIncludingHash("index", EntityIndex.create(expectedColumns), index);

        isUnmodifiable("customer validation", customer.getValidation());
        assertEquals("customer validations",
                newHashSet(
                        unique(asList(asList("customer", "last_name"), asList("customer", "first_name"))),
                        unique(asList(asList("orders", "placed")))
                ),
                set(customer.getValidation())
        );

        Attribute customerId = customer.getAttributes().get("id");
        assertEquals("id class", Attribute.AttributeType.SCALAR, customerId.getAttributeType());
        assertEquals("id uuid", UUID.fromString("8644c36b-f881-4369-a06b-59e3fc580309"), customerId.getUUID());
        assertEquals("id type", "integer", customerId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), customerId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(customerId.getValidation()));
        assertEquals("id spine position", 0, customerId.getSpinePos());
        assertNull("id attribute", customerId.getAttributes());

        Attribute lastName = customer.getAttributes().get("last_name");
        assertEquals("last_name class", Attribute.AttributeType.SCALAR, lastName.getAttributeType());
        assertEquals("last_name uuid", UUID.fromString("257e9b59-e77f-4a4d-a5da-00c7c2261875"), lastName.getUUID());
        assertEquals("last_name type", "varchar", lastName.getType());
        assertEquals("last_name properties", map("charset", "utf8"), lastName.getProperties());
        assertEquals("last_name validations",
                newHashSet(new Validation("required", false), new Validation("max_length", 64)),
                set(lastName.getValidation()));
        assertEquals("last_name spine pos", -1, lastName.getSpinePos());
        assertNull("last_name attribute", lastName.getAttributes());
        
        Attribute orders = customer.getAttributes().get("orders");
        assertEquals("orders class", Attribute.AttributeType.COLLECTION, orders.getAttributeType());
        assertEquals("orders uuid", UUID.fromString("c5cedd91-9751-41c2-9417-8c29117ca2c9"), orders.getUUID());
        assertNull("orders type", orders.getType());
        assertEquals("orders properties empty", true, orders.getProperties().isEmpty());
        assertEquals("orders validations empty", true, orders.getValidation().isEmpty());
        assertEquals("orders spine pos", -1, orders.getSpinePos());
        isUnmodifiable("orders attributes", orders.getAttributes());
        assertEquals("orders attributes key", set("id"), orders.getAttributes().keySet());


        Attribute orderId = orders.getAttributes().get("id");
        assertEquals("id class", Attribute.AttributeType.SCALAR, orderId.getAttributeType());
        assertEquals("id uuid", UUID.fromString("58dd53b7-e8a1-488b-a751-c83f9beca04c"), orderId.getUUID());
        assertEquals("id type", "integer", orderId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), orderId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(orderId.getValidation()));
        assertEquals("id spine pos", 0, orderId.getSpinePos());
        assertNull("id attribute", orderId.getAttributes());
    }

    @Test
    public void visitor() {
        List<String> expected = asList(
                "visiting entity: [customer, entity {2a57b59e-e1b7-4377-996e-a5c04e0abf29}]",
                "visiting scalar: [id, scalar {8644c36b-f881-4369-a06b-59e3fc580309}]",
                "visiting scalar: [last_name, scalar {257e9b59-e77f-4a4d-a5da-00c7c2261875}]",
                "visiting collection: [orders, collection {c5cedd91-9751-41c2-9417-8c29117ca2c9}]",
                "visiting scalar: [id, scalar {58dd53b7-e8a1-488b-a751-c83f9beca04c}]",
                "leaving collection",
                "visiting entity validation: [unique: [[customer, last_name], [customer, first_name]]]",
                "visiting entity validation: [unique: [[orders, placed]]]",
                "visiting index: [orderplaced_lastname, [orders.placed, customer.last_name]]",
                "leaving entity"
        );
        ToStringVisitor visitor = new ToStringVisitor();
        Space.readSpace("coi.json", SpaceTest.class, null).visit(visitor);
        assertCollectionEquals("messages", expected, visitor.getMessages());
    }

    private static Set<String> set(String... elements) {
        return newHashSet(elements);
    }

    private static <T> Set<T> set(Collection<? extends T> collection) {
        Set<T> set = new HashSet<>(collection);
        if(set.size() != collection.size())
            throw new IllegalArgumentException("incoming collection has non-unique elements");
        return set;
    }

    private static Validation unique(List<List<String>> columns) {
        return new Validation("unique", columns);
    }

}
