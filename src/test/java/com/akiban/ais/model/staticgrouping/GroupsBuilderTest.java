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

package com.akiban.ais.model.staticgrouping;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.util.List;

import com.akiban.ais.model.TableName;
import org.junit.Test;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.Index;

public class GroupsBuilderTest {
    private final String SCHEMA = "test_schema";

    @Test
    public void buildTwoGroups() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_a");
        builder.rootTable(SCHEMA, "pages", "group_b");
        Grouping grouping = builder.getGrouping();

        assertEquals("group default schema", SCHEMA, grouping.getDefaultSchema());

        List<Group> groups = grouping.getGroups();
        assertEquals("groups size", 2, groups.size());
        
        assertSimilarGroup("first", new TableName(SCHEMA, "group_a"), groups.get(0));
        assertSimilarGroup("second", new TableName(SCHEMA, "group_b"), groups.get(1));
    }

    @Test(expected=IllegalStateException.class)
    public void conflictingGroupNames() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        builder.rootTable(SCHEMA, "customers", "group_a");
        builder.rootTable(SCHEMA, "pages", "group_a");
    }

    @Test(expected=IllegalStateException.class)
    public void unfinishedColumn() {
        GroupsBuilder builder = null;
        try {
            builder = new GroupsBuilder(SCHEMA);
            builder.rootTable(SCHEMA, "customers", "group_0");
            builder.joinTables(SCHEMA, "customers", SCHEMA, "orders"); // note that I'm not defining any columns!
        }
        catch (Throwable t) {
            fail("wrong exception: " + t);
        }
        builder.getGrouping();
    }

    @Test(expected=IllegalStateException.class)
    public void joinChildIsRoot() {
        GroupsBuilder builder = null;
        try {
            builder = new GroupsBuilder(SCHEMA);
            builder.rootTable(SCHEMA, "customers", "group_0");
            builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
            builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid");
        }
        catch (Throwable t) {
            fail("wrong exception: " + t);
        }
        builder.joinTables(SCHEMA, "items", SCHEMA, "customers").column("id", "item_id");
    }

    @Test(expected=IllegalStateException.class)
    public void joinChildIsElsewhere() {
        GroupsBuilder builder = null;
        try {
            builder = new GroupsBuilder(SCHEMA);
            builder.rootTable(SCHEMA, "customers", "group_0");
            builder.joinTables(SCHEMA, "customers", SCHEMA, "orders").column("id", "cid");
            builder.joinTables(SCHEMA, "orders", SCHEMA, "items").column("id", "oid");
        }
        catch (Throwable t) {
            fail("wrong exception: " + t);
        }
        builder.joinTables(SCHEMA, "items", SCHEMA, "orders").column("id", "item_id");
    }

    @Test
    public void fromAIS() {
        // We'll give the joins in the wrong order -- which shouldn't matter.
        // (AIS stores its Joins in a TreeMap, so we can control the order they get to the builder)
        AISBuilder aisBuilder = new AISBuilder();

        aisBuilder.userTable("s", "customer");
        aisBuilder.column("s", "customer", "id", 0, "INT", 4L, null, false, true, null, null);
        aisBuilder.index("s", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s", "customer", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);

        aisBuilder.userTable("s", "order");
        aisBuilder.column("s", "order", "id", 0, "INT", 4L, null, false, true, null, null);
        aisBuilder.column("s", "order", "cid", 1, "INT", 4L, null, false, false, null, null);
        aisBuilder.index("s", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s", "order", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        aisBuilder.joinTables("join3", "s", "customer", "s", "order");
        aisBuilder.joinColumns("join3", "s", "customer", "id", "s", "order", "cid");

        aisBuilder.userTable("s", "item");
        aisBuilder.column("s", "item", "id", 0, "INT", 4L, null, false, true, null, null);
        aisBuilder.column("s", "item", "oid", 1, "INT", 4L, null, false, false, null, null);
        aisBuilder.index("s", "item", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s", "item", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        aisBuilder.joinTables("join2", "s", "order", "s", "item");
        aisBuilder.joinColumns("join2", "s", "order", "id", "s", "item", "oid");

        aisBuilder.userTable("s2", "customer");
        aisBuilder.column("s2", "customer", "id", 0, "INT", 4L, null, false, true, null, null);
        aisBuilder.index("s2", "customer", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s2", "customer", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);

        aisBuilder.basicSchemaIsComplete();
        aisBuilder.createGroup("group1", "group_schema");
        aisBuilder.addTableToGroup("group1", "s", "customer");
        aisBuilder.addJoinToGroup("group1", "join3", 1);
        aisBuilder.addJoinToGroup("group1", "join2", 1);
        aisBuilder.createGroup("group2", "group_schema");
        aisBuilder.addTableToGroup("group2", "s2", "customer");
        aisBuilder.groupingIsComplete();

        GroupsBuilder expectedGrouping = new GroupsBuilder("s");
        expectedGrouping.rootTable("s", "customer", "group1");
        expectedGrouping.joinTables("s", "customer", "s", "order").column("id", "cid");
        expectedGrouping.joinTables("s", "order", "s", "item").column("id", "oid");
        expectedGrouping.rootTable("s2", "customer", "group2");

        Grouping actualGrouping = GroupsBuilder.fromAis(aisBuilder.akibanInformationSchema(), "s");
        assertEquals("groupings", expectedGrouping.getGrouping().toString(), actualGrouping.toString());
    }

    @Test
    public void noGroups() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        Grouping grouping = builder.getGrouping();
        assertEquals("grouping", "groupschema " + SCHEMA, grouping.toString().trim());
    }

    private static void assertSimilarGroup(String message, TableName expectedName,
                                           Group actualGroup)
    {
        assertEquals(message + " group name", expectedName, actualGroup.getGroupName());
    }
}
