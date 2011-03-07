/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.mttests.mthapi.base.sais;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.Assert.*;

public final class SaisBuilderTest {
    @Test
    public void basicTest() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("customer", "id", "name");
        builder.table("order", "id", "cid").joinTo("customer").col("id", "cid");
        builder.table("address", "aid", "cid").joinTo("customer").col("id", "cid");
        builder.table("zoo", "id", "zebra");

        Map<String,SaisTable> roots = tablesByName(builder.getRootTables());

        assertEquals("root names", stringSet("customer", "zoo"), roots.keySet());

        SaisTable customer = roots.get("customer");
        assertEquals("customer fields", stringSet("id", "name"), customer.getFields());
        Map<String,SaisFK> cFKs = fksByChild(customer);
        assertEquals("customer children", stringSet("order", "address"), cFKs.keySet());

        SaisFK orderFK = cFKs.get("order");
        assertEquals("orderFK child", "order", orderFK.getChild().getName());
        assertEquals("orderFK fields", stringToString("id", "cid"), orderFK.getFkPairs());
        leafTable(orderFK.getChild(), "order", "id", "cid");
        assertSame("order table", orderFK.getChild(), customer.getChild("order"));

        SaisFK addressFK = cFKs.get("address");
        assertEquals("addressFK child", "address", addressFK.getChild().getName());
        assertEquals("addressFK fields", stringToString("id", "cid"), addressFK.getFkPairs());
        leafTable(addressFK.getChild(), "address", "aid", "cid");
        assertSame("address table", addressFK.getChild(), customer.getChild("address"));

        leafTable(roots.get("zoo"), "zoo", "id", "zebra");
    }

    @Test
    public void soleRoot() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("ego", "id");
        SaisTable freud = builder.getSoleRootTable();
        leafTable(freud, "ego", "id");
    }

    @Test(expected=IllegalArgumentException.class)
    public void definedTwice() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("one", "id1", "id2");
        builder.table("one", "id1", "id2");
    }

    @Test(expected=NoSuchElementException.class)
    public void joinToUnknown() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("order", "oid", "cid").joinTo("customer").col("cid", "cid");
    }

    @Test(expected=IllegalArgumentException.class)
    public void emptyJoin() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("zoo", "id");
        builder.table("zebra", "id").joinTo("zoo");
        builder.getRootTables();
    }

    @Test(expected=IllegalArgumentException.class)
    public void joinToSelf() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("self", "id").joinTo("self").col("id", "id");
    }

    @Test(expected=IllegalStateException.class)
    public void soleHasTooMany() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("foo", "id");
        builder.table("bar", "id");
        builder.getSoleRootTable();
    }

    static void leafTable(SaisTable table, String name, String... fields) {
        assertEquals(name, table.getName());
        assertEquals(name + " fields", stringSet(fields), table.getFields());
        assertEquals(name + " fks " + table.getChildren(), 0, table.getChildren().size());
    }

    static Map<String,String> stringToString(String key, String value) {
        Map<String,String> map = new HashMap<String, String>();
        map.put(key, value);
        return map;
    }

    static Map<String,SaisTable> tablesByName(Set<SaisTable> set) {
        Map<String,SaisTable> map = new HashMap<String, SaisTable>(set.size());
        for (SaisTable table : set) {
            map.put(table.getName(), table);
        }
        return map;
    }

    static Map<String,SaisFK> fksByChild(SaisTable parent) {
        Set<SaisFK> set = parent.getChildren();
        Map<String,SaisFK> map = new HashMap<String, SaisFK>(set.size());
        for (SaisFK fk : set) {
            SaisTable childTable = fk.getChild();
            assertSame("child's parent FK", fk, childTable.getParentFK());
            assertSame("child's parent", parent, fk.getParent());
            map.put(childTable.getName(), fk);
        }
        return map;
    }

    static Set<String> stringSet(String... strings) {
        return new HashSet<String>(Arrays.asList(strings));
    }
}
