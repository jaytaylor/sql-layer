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
            .userTable("order").colLong("oid").colLong("c2").colLong("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .userTable("item").colLong("iid").colLong("o_id").colLong("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
            .userTable("address").colLong("aid").colLong("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
            .ais();

        Grouping actualGrouping = GroupsBuilder.fromAis(ais, "sch");

        GroupsBuilder expectedGrouping = new GroupsBuilder("sch");
        expectedGrouping.rootTable("sch", "customer", "customer");
        // address goes first, since GroupsBuilder.fromAis goes in alphabetical order
        expectedGrouping.joinTables("sch", "customer", "sch", "address").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "customer", "sch", "order").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "order", "sch", "item").column("oid", "o_id").column("c2", "o_c2");

        assertEquals("grouping string", expectedGrouping.getGrouping().toString(), actualGrouping.toString());
    }
    @Test
    public void exampleWithGroupIndexes() {
        NewAISBuilder builder = AISBBasedBuilder.create();
        AkibanInformationSchema ais =
                builder.defaultSchema("sch")
                        .userTable("customer").colLong("cid").colString("name", 32).pk("cid")
                        .userTable("order").colLong("oid").colLong("c2").colLong("c_id").pk("oid", "c2").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                        .userTable("item").colLong("iid").colLong("o_id").colLong("o_c2").key("o_id", "o_id", "o_c2").joinTo("order").on("o_id", "oid").and("o_c2", "c2")
                        .userTable("address").colLong("aid").colLong("c_id").key("c_id", "c_id").joinTo("customer").on("c_id", "cid")
                .groupIndex("name_c2").on("customer", "name").and("order", "c2")
                .groupIndex("iid_name_c2").on("item", "iid").and("customer", "name").and("order", "c2")
                        .ais();

        Grouping actualGrouping = GroupsBuilder.fromAis(ais, "sch");

        GroupsBuilder expectedGrouping = new GroupsBuilder("sch");
        expectedGrouping.rootTable("sch", "customer", "customer");
        // address goes first, since GroupsBuilder.fromAis goes in alphabetical order
        expectedGrouping.joinTables("sch", "customer", "sch", "address").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "customer", "sch", "order").column("cid", "c_id");
        expectedGrouping.joinTables("sch", "order", "sch", "item").column("oid", "o_id").column("c2", "o_c2");

        assertEquals("grouping string", expectedGrouping.getGrouping().toString(), actualGrouping.toString());
    }
}
