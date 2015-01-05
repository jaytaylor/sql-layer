/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.qp.operator;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.OnlyIf;
import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
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
        // bitmap descriptors are leaf to root
        param(builder, IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.o), "010");
        param(builder, IndexScanSelector.leftJoinAfter(ais.oiGroupIndex, ais.i), "110");
        param(builder, IndexScanSelector.rightJoinUntil(ais.oiGroupIndex, ais.o), "110");
        param(builder, IndexScanSelector.rightJoinUntil(ais.oiGroupIndex, ais.i), "100");
        param(builder, IndexScanSelector.inner(ais.oiGroupIndex), "110");

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
            TypesTranslator typesTranslator = MTypesTranslator.INSTANCE;
            AkibanInformationSchema ais = AISBBasedBuilder.create("coih", typesTranslator)
                    .table("customers").colInt("cid").colString("name", 32).pk("cid")
                    .table("orders").colInt("oid").colInt("c_id").colInt("priority").pk("oid")
                        .key("o_index", "priority")
                        .joinTo("customers").on("c_id", "cid")
                    .table("items").colInt("iid").colInt("o_id").colInt("sku").pk("iid")
                        .joinTo("orders").on("o_id", "oid")
                    .table("handling").colInt("hid").colInt("i_id").colString("description", 32)
                        .joinTo("items").on("i_id", "iid")
                    .groupIndex("sku_priority_gi", Index.JoinType.LEFT).on("items", "sku").and("orders", "priority")
                    .ais();
            c = ais.getTable("coih", "customers");
            o = ais.getTable("coih", "orders");
            i = ais.getTable("coih", "items");
            h = ais.getTable("coih", "handling");
            oTableIndex = o.getIndex("o_index");
            oiGroupIndex = i.getGroup().getIndex("sku_priority_gi");
        }


        private final Table c;
        private final Table o;
        private final Table i;
        private final Table h;
        private final Index oiGroupIndex;
        private final Index oTableIndex;
    }
}
