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
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class SaisTableTest {
    @Test
    public void countEmptyChildren() {
        SaisTable sole = new SaisBuilder().table("sole", "id").backToBuilder().getSoleRootTable();
        assertEquals("count", 1, sole.countIncludingChildren());
    }

    @Test
    public void countCOIA() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        SaisTable customer = builder.getSoleRootTable();
        assertEquals("count", 4, customer.countIncludingChildren());
    }

    @Test
    public void setCOIA() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "cid").pk("cid");
        builder.table("o", "oid", "c_id").pk("oid").joinTo("c").col("cid", "c_id");
        builder.table("i", "iid", "o_id").joinTo("o").col("oid", "o_id");
        builder.table("a", "aid", "c_id").pk("aid").joinTo("c").col("cid", "c_id");
        SaisTable customer = builder.getSoleRootTable();
        SaisTable order = customer.getChild("o");
        SaisTable item = order.getChild("i");
        SaisTable address = customer.getChild("a");

        Set<SaisTable> expectedSet = new HashSet<SaisTable>(Arrays.asList(customer, order, item, address));
        Set<SaisTable> actualSet = customer.setIncludingChildren();

        assertEquals("sets by equality", identityHashMap(expectedSet), identityHashMap(actualSet));
    }
    
    private static IdentityHashMap<SaisTable,String> identityHashMap(Set<SaisTable> tables) {
        IdentityHashMap<SaisTable,String> map = new IdentityHashMap<SaisTable, String>();
        for (SaisTable table : tables) {
            map.put(table, table.getName());
        }
        return map;
    }
}
