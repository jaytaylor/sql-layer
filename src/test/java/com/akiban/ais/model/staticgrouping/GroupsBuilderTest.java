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

package com.akiban.ais.model.staticgrouping;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.fail;

import java.util.List;

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
        
        assertSimilarGroup("first", "group_a", groups.get(0));
        assertSimilarGroup("second", "group_b", groups.get(1));
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
        aisBuilder.column("s", "order", "cid", 0, "INT", 4L, null, false, false, null, null);
        aisBuilder.index("s", "order", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s", "order", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        aisBuilder.joinTables("join3", "s", "customer", "s", "order");
        aisBuilder.joinColumns("join3", "s", "customer", "id", "s", "order", "cid");

        aisBuilder.userTable("s", "item");
        aisBuilder.column("s", "item", "id", 0, "INT", 4L, null, false, true, null, null);
        aisBuilder.column("s", "item", "oid", 0, "INT", 4L, null, false, false, null, null);
        aisBuilder.index("s", "item", Index.PRIMARY_KEY_CONSTRAINT, true, Index.PRIMARY_KEY_CONSTRAINT);
        aisBuilder.indexColumn("s", "item", Index.PRIMARY_KEY_CONSTRAINT, "id", 0, true, null);
        aisBuilder.joinTables("join2", "s", "order", "s", "item");
        aisBuilder.joinColumns("join2", "s", "order", "id", "s", "item", "oid");

        aisBuilder.basicSchemaIsComplete();
        aisBuilder.createGroup("group1", "akiba_objects", "_akiba_customer");
        aisBuilder.addTableToGroup("group1", "s", "customer");
        aisBuilder.addJoinToGroup("group1", "join3", 1);
        aisBuilder.addJoinToGroup("group1", "join2", 1);

        GroupsBuilder expectedGrouping = new GroupsBuilder("s");
        expectedGrouping.rootTable("s", "customer", "group1");
        expectedGrouping.joinTables("s", "customer", "s", "order").column("id", "cid");
        expectedGrouping.joinTables("s", "order", "s", "item").column("id", "oid");

        Grouping actualGrouping = GroupsBuilder.fromAis(aisBuilder.akibaInformationSchema(), "s");
        
        assertEquals("groupings", expectedGrouping.getGrouping().toString(), actualGrouping.toString());

    }

    @Test
    public void noGroups() {
        GroupsBuilder builder = new GroupsBuilder(SCHEMA);
        Grouping grouping = builder.getGrouping();
        assertEquals("grouping", "groupschema " + SCHEMA, grouping.toString().trim());
    }

    private static void assertSimilarGroup(String message, String expectedName,
                                           Group actualGroup)
    {
        assertEquals(message + " group name", expectedName, actualGroup.getGroupName());
    }
}
