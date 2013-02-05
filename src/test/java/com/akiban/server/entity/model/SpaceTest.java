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

package com.akiban.server.entity.model;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public final class SpaceTest {
    @Test()
    public void coi() {
        // space
        Space space = getSpace("coi.json");
        isUnmodifiable("space", space.getEntities());
        assertEquals("space keys", set("customer"), space.getEntities().keySet());

        Entity customer = space.getEntities().get("customer");
        assertEquals("customer uuid", UUID.fromString("2a57b59e-e1b7-4377-996e-a5c04e0abf29"), customer.uuid());
        isUnmodifiable("customer attributes", customer.getAttributes());
        assertEquals("customer attributes", set("id", "last_name", "orders"), customer.getAttributes().keySet());

        isUnmodifiable("customer indexes", customer.getIndexes());
        assertEquals("customer index names",
                set("orderplaced_lastname"),
                set(customer.getIndexes().keySet()));
        EntityIndex index = customer.getIndexes().get("orderplaced_lastname");
        List<EntityColumn> expectedColumns = asList(
                new EntityColumn("orders", "placed"), new EntityColumn("customer", "last_name")
        );
        isUnmodifiable("index columns", index.getColumns());
        assertEquals("index columns", expectedColumns, index.getColumns());
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
        assertEquals("id type", "int", customerId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), customerId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(customerId.getValidation()));
        assertTrue("id is not ID", customerId.isId());
        assertNull("id attribute", customerId.getAttributes());

        Attribute lastName = customer.getAttributes().get("last_name");
        assertEquals("last_name class", Attribute.AttributeType.SCALAR, lastName.getAttributeType());
        assertEquals("last_name uuid", UUID.fromString("257e9b59-e77f-4a4d-a5da-00c7c2261875"), lastName.getUUID());
        assertEquals("last_name type", "varchar", lastName.getType());
        assertEquals("last_name properties", map("charset", "utf8"), lastName.getProperties());
        assertEquals("last_name validations",
                newHashSet(new Validation("required", false), new Validation("max_length", 64)),
                set(lastName.getValidation()));
        assertFalse("last_name is ID", lastName.isId());
        assertNull("last_name attribute", lastName.getAttributes());
        
        Attribute orders = customer.getAttributes().get("orders");
        assertEquals("orders class", Attribute.AttributeType.COLLECTION, orders.getAttributeType());
        assertEquals("orders uuid", UUID.fromString("c5cedd91-9751-41c2-9417-8c29117ca2c9"), orders.getUUID());
        assertNull("orders type", orders.getType());
        assertNull("orders properties", orders.getProperties());
        assertNull("orders validations", orders.getValidation());
        assertFalse("orders is ID", orders.isId());
        isUnmodifiable("orders attributes", orders.getAttributes());
        assertEquals("orders attributes key", set("id"), orders.getAttributes().keySet());


        Attribute orderId = orders.getAttributes().get("id");
        assertEquals("id class", Attribute.AttributeType.SCALAR, orderId.getAttributeType());
        assertEquals("id uuid", UUID.fromString("58dd53b7-e8a1-488b-a751-c83f9beca04c"), orderId.getUUID());
        assertEquals("id type", "int", orderId.getType());
        assertEquals("id properties", Collections.<String, Object>emptyMap(), orderId.getProperties());
        assertEquals("id validations", Collections.<Validation>emptySet(), set(orderId.getValidation()));
        assertTrue("id is not ID", orderId.isId());
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
        getSpace("coi.json").visit(visitor);
        assertCollectionEquals("messages", expected, visitor.getMessages());
    }

    static Space getSpace(String fileName) {
        try (InputStream is = SpaceTest.class.getResourceAsStream(fileName)) {
            if (is == null) {
                throw new RuntimeException("resource not found: " + fileName);
            }
            Reader reader = new BufferedReader(new InputStreamReader(is, "utf-8"));
            return Space.create(reader);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
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
