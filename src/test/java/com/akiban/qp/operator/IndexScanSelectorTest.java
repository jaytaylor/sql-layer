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

package com.akiban.qp.operator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(NamedParameterizedRunner.class)
public final class IndexScanSelectorTest {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ParameterizationBuilder builder = new ParameterizationBuilder();

        builder.add("ONCE", null, null);
        param(builder, IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.o), "10");
        param(builder, IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.i), "11");
        param(builder, IndexScanSelector.rightJoinUntil(ais.oiGroupIndex, ais.o), "11");
        param(builder, IndexScanSelector.rightJoinUntil(ais.oiGroupIndex, ais.i), "01");
        param(builder, IndexScanSelector.inner(ais.oiGroupIndex), "11");

        return builder.asList();
    }

    private static void param(ParameterizationBuilder builder, IndexScanSelector selector, String bitmap) {
        bitmap = bitmap.substring(bitmap.indexOf('1'));
        for(char c : bitmap.toCharArray())
            assert c == '1' || c == '0' : c;
        builder.add(builder.asList().size() + " " + selector.describe() + " -> " + bitmap, selector, bitmap);
    }

    @OnlyIf("parameterized()")
    @Test
    public void testBitMap() {
        String actual = Long.toBinaryString(selector.getBitMask());
        assertEquals(expectedMap, actual);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void leftAboveGI() {
        IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.c);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void leftBelowGI() {
        IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.h);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void rightAboveGI() {
        IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.c);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void rightBelowGI() {
        IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.h);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void leftBelowTI() {
        IndexScanSelector.leftJoinAfter(ais.oTableIndex, ais.i);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void leftAboveTI() {
        IndexScanSelector.leftJoinAfter(ais.oTableIndex, ais.c);
    }

    @OnlyIfNot("parameterized()")
    @Test()
    public void leftAtTI() {
        assertTrue("doesn't match all", IndexScanSelector.leftJoinAfter(ais.oTableIndex, ais.o).matchesAll());
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void rightBelowTI() {
        IndexScanSelector.rightJoinUntil(ais.oTableIndex, ais.i);
    }

    @OnlyIfNot("parameterized()")
    @Test(expected = IllegalArgumentException.class)
    public void rightAboveTI() {
        IndexScanSelector.rightJoinUntil(ais.oTableIndex, ais.c);
    }

    @OnlyIfNot("parameterized()")
    @Test()
    public void rightAtTI() {
        assertTrue("doesn't match all", IndexScanSelector.rightJoinUntil(ais.oTableIndex, ais.o).matchesAll());
    }

    public boolean parameterized() {
        return selector != null;
    }

    public IndexScanSelectorTest(IndexScanSelector selector, String expectedMap) {
        this.selector = selector;
        this.expectedMap = expectedMap;
    }

    private final IndexScanSelector selector;
    private final String expectedMap;
    private static final AisStruct ais = new AisStruct();

    private static class AisStruct {
        public AisStruct() {
            AkibanInformationSchema ais = AISBBasedBuilder.create("coih")
                    .userTable("customers").colLong("cid").colString("name", 32).pk("cid")
                    .userTable("orders").colLong("oid").colLong("c_id").colLong("priority").pk("oid")
                        .key("o_index", "priority")
                        .joinTo("customers").on("c_id", "cid")
                    .userTable("items").colLong("iid").colLong("o_id").colLong("sku").pk("iid")
                        .joinTo("orders").on("o_id", "oid")
                    .userTable("handling").colLong("hid").colLong("i_id").colString("description", 32)
                        .joinTo("items").on("i_id", "iid")
                    .groupIndex("sku_priority_gi").on("items", "sku").and("orders", "priority")
                    .ais();
            c = ais.getUserTable("coih", "customers");
            o = ais.getUserTable("coih", "orders");
            i = ais.getUserTable("coih", "items");
            h = ais.getUserTable("coih", "handling");
            oTableIndex = o.getIndex("o_index");
            oiGroupIndex = i.getGroup().getIndex("sku_priority_gi");
        }


        private final UserTable c;
        private final UserTable o;
        private final UserTable i;
        private final UserTable h;
        private final Index oiGroupIndex;
        private final Index oTableIndex;
    }
}
