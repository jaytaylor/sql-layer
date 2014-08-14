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

package com.foundationdb.ais.model.aisb2;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.Index;
import com.foundationdb.server.types.common.types.TypesTranslator;
import com.foundationdb.server.types.mcompat.mtypes.MTypesTranslator;
import org.junit.Test;

import static org.junit.Assert.*;

public class AISBBasedBuilderTest {
    protected TypesTranslator typesTranslator() {
        return MTypesTranslator.INSTANCE;
    }

    @Test
    public void example() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        AkibanInformationSchema ais =
            builder.defaultSchema("sch")
            .table("customer").colInt("cid").colString("name", 32).pk("cid")
            .table("order").colInt("oid").colInt("c2").colInt("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .table("item").colInt("iid").colInt("o_id").colInt("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
            .table("address").colInt("aid").colInt("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .ais();

        Group cGroup = ais.getGroup(new TableName("sch", "customer"));
        Table cTable = ais.getTable("sch", "customer");
        Table aTable = ais.getTable("sch", "address");
        Table oTable = ais.getTable("sch", "order");
        Table iTable = ais.getTable("sch", "item");

        assertNotNull("customer group", cGroup);
        assertEquals("customer group root", cGroup.getRoot(), cTable);

        assertEquals("address parent", cTable, aTable.getParentJoin().getParent());
        assertEquals("address join", "[JoinColumn(c_id -> cid)]", aTable.getParentJoin().getJoinColumns().toString());

        assertEquals("order parent", cTable, oTable.getParentJoin().getParent());
        assertEquals("order join", "[JoinColumn(c_id -> cid)]", oTable.getParentJoin().getJoinColumns().toString());

        assertEquals("item parent", oTable, iTable.getParentJoin().getParent());
        assertEquals("item join", "[JoinColumn(o_id -> oid), JoinColumn(o_c2 -> c2)]", iTable.getParentJoin().getJoinColumns().toString());
    }

    @Test
    public void exampleWithGroupIndexes() {
        NewAISBuilder builder = AISBBasedBuilder.create(typesTranslator());
        AkibanInformationSchema ais =
                builder.defaultSchema("sch")
                        .table("customer").colInt("cid").colString("name", 32).pk("cid")
                        .table("order").colInt("oid").colInt("c2").colInt("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                        .table("item").colInt("iid").colInt("o_id").colInt("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
                        .table("address").colInt("aid").colInt("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                .groupIndex("name_c2", Index.JoinType.LEFT).on("customer", "name").and("order", "c2")
                .groupIndex("iid_name_c2", Index.JoinType.LEFT).on("item", "iid").and("customer", "name").and("order", "c2")
                        .ais();

        Group cGroup = ais.getGroup(new TableName("sch", "customer"));
        Table cTable = ais.getTable("sch", "customer");
        Table aTable = ais.getTable("sch", "address");
        Table oTable = ais.getTable("sch", "order");
        Table iTable = ais.getTable("sch", "item");

        assertNotNull("customer group", cGroup);
        assertEquals("customer group root", cGroup.getRoot(), cTable);

        assertEquals("address parent", cTable, aTable.getParentJoin().getParent());
        assertEquals("address join", "[JoinColumn(c_id -> cid)]", aTable.getParentJoin().getJoinColumns().toString());

        assertEquals("order parent", cTable, oTable.getParentJoin().getParent());
        assertEquals("order join", "[JoinColumn(c_id -> cid)]", oTable.getParentJoin().getJoinColumns().toString());

        assertEquals("item parent", oTable, iTable.getParentJoin().getParent());
        assertEquals("item join", "[JoinColumn(o_id -> oid), JoinColumn(o_c2 -> c2)]", iTable.getParentJoin().getJoinColumns().toString());
    }
}
