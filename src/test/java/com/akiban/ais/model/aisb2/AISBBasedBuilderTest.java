package com.akiban.ais.model.aisb2;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.staticgrouping.Grouping;
import com.akiban.ais.model.staticgrouping.GroupsBuilder;
import org.junit.Test;

import static org.junit.Assert.*;

public class AISBBasedBuilderTest {
    @Test
    public void example() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        AkibanInformationSchema ais =
            builder.defaultSchema("sch")
            .userTable("customer").colLong("cid").colString("name", 32).pk("cid")
            .userTable("order").colLong("oid").colLong("c_id").pk("oid").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .userTable("item").colLong("iid").colLong("o_id").key("o_id", "o_id").joinTo("order").on("o_id", "oid")
            .userTable("address").colLong("aid").colLong("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .ais();

        Grouping actualGrouping = GroupsBuilder.fromAis(ais, "sch");

        GroupsBuilder expectedGrouping = new GroupsBuilder("sch");
        expectedGrouping.rootTable("sch", "customer", "customer");
        // address goes first, since GroupsBuilder.fromAis goes in alphabetical order
        expectedGrouping.joinTables("sch", "customer", "sch", "address").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "customer", "sch", "order").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "order", "sch", "item").column("oid", "o_id");

        assertEquals("grouping string", expectedGrouping.getGrouping().toString(), actualGrouping.toString());

    }
}
