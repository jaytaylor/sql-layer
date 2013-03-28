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

import java.util.ArrayList;
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public final class SpaceTest {
    @Test()
    public void coi() {
        // space
        Space space = Space.readSpace("coi.json", SpaceTest.class, null);
        isUnmodifiable("space", space.getEntities());
        assertEquals("space keys", asList("customer"), elementNames(space.getEntities()));

        Entity customer = findElement(space.getEntities(), "customer");
        assertEquals("customer identifying", asList("id"), customer.getIdentifying());
        assertEquals("customer uuid", UUID.fromString("2a57b59e-e1b7-4377-996e-a5c04e0abf29"), customer.getUuid());
        isUnmodifiable("customer attributes", customer.getFields());
        isUnmodifiable("customer attributes", customer.getCollections());
        assertEquals("customer fields", set("id", "last_name", "orders"), elementNames(customer.getFields()));
        assertEquals("customer collections", set("orders"), elementNames(customer.getCollections()));

        isUnmodifiable("customer indexes", customer.getIndexes());
        assertEquals("customer index names",
                set("orderplaced_lastname"),
                set(customer.getIndexes().keySet()));
        EntityIndex index = customer.getIndexes().get("orderplaced_lastname");
        List<? extends IndexField> expectedColumns = asList(
                new IndexField.QualifiedFieldName("orders", "placed"),
                new IndexField.QualifiedFieldName("customer", "last_name")
        );
        isUnmodifiable("index columns", index.getFields());
        assertEquals("index columns", expectedColumns, index.getFields());
        equalsIncludingHash("index", EntityIndex.create(expectedColumns), index);

        isUnmodifiable("customer validation", customer.getValidations());
        assertEquals("customer validations",
                newHashSet(
                        unique(asList(asList("customer", "last_name"), asList("customer", "first_name"))),
                        unique(asList(asList("orders", "placed")))
                ),
                set(customer.getValidations())
        );

        EntityField customerId = findElement(customer.getFields(), "id");
        assertEquals("id uuid", UUID.fromString("8644c36b-f881-4369-a06b-59e3fc580309"), customerId.getUuid());
        assertEquals("id type", "integer", customerId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), customerId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(customerId.getValidations()));

        EntityField lastName = findElement(customer.getFields(), "last_name");
        assertEquals("last_name uuid", UUID.fromString("257e9b59-e77f-4a4d-a5da-00c7c2261875"), lastName.getUuid());
        assertEquals("last_name type", "varchar", lastName.getType());
        assertEquals("last_name properties", map("charset", (Object)"utf8"), lastName.getProperties());
        assertEquals("last_name validations",
                newHashSet(new Validation("required", false), new Validation("max_length", 64)),
                set(lastName.getValidations()));
        
        EntityCollection orders = findElement(customer.getCollections(), "orders");
        assertEquals("orders identifying", asList("id"), orders.getIdentifying());
        assertEquals("orders grouping fields", asList("cid"), orders.getGroupingFields());
        assertEquals("orders uuid", UUID.fromString("c5cedd91-9751-41c2-9417-8c29117ca2c9"), orders.getUuid());
        isUnmodifiable("orders attributes", orders.getCollections());
        assertEquals("orders attributes key", set(), elementNames(orders.getCollections()));
        assertEquals("orders attributes key", set("id"), elementNames(orders.getFields()));

        EntityField orderId = findElement(orders.getFields(), "id");
        assertEquals("id uuid", UUID.fromString("58dd53b7-e8a1-488b-a751-c83f9beca04c"), orderId.getUuid());
        assertEquals("id type", "integer", orderId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), orderId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(orderId.getValidations()));
    }

    private <T extends EntityElement> T findElement(Collection<T> elements, String name) {
        T found = null;
        for (T entity : elements) {
            if (name.equals(entity.getName())) {
                assertNull("multiple elements named " + name, found);
                found = entity;
            }
        }
        assertNotNull("no element found named " + name, found);
        return found;
    }

    private Collection<String> elementNames(Collection<? extends EntityElement> elements) {
        List<String> names = new ArrayList<>(elements.size());
        for (EntityElement elem : elements)
            names.add(elem.getName());
        return names;
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
