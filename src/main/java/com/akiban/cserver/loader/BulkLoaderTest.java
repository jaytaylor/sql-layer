package com.akiban.cserver.loader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class BulkLoaderTest extends TestCase {
    @Override
    protected void setUp() throws Exception {
        System.out.println(">>> " + getName());
    }

    public void testDummy() {
    }

    public void testCOI() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false,
                false);
        builder.column("schema", "customer", "customer_name", 1, "varchar",
                64L, 0L, false, false);
        builder.index("schema", "customer", "customer_pk", true, "PRIMARY KEY");
        builder
                .indexColumn("schema", "customer", "customer_pk", "cid", 0,
                        true);
        builder.userTable("schema", "order");
        builder
                .column("schema", "order", "oid", 0, "int", 0L, 0L, false,
                        false);
        builder
                .column("schema", "order", "cid", 1, "int", 0L, 0L, false,
                        false);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L,
                false, false);
        builder.index("schema", "order", "order_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "order", "order_pk", "oid", 0, true);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false,
                false);
        builder.index("schema", "item", "item_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "item", "item_pk", "iid", 0, true);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema",
                "order", "cid");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "order",
                "oid");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        TestActions actions = new TestActions();
        BulkLoader bulkLoader = new BulkLoader(ais, "group", "bulkload",
                actions);
        bulkLoader.run();
        Assert.assertEquals(Arrays.asList(ais
                .getUserTable("schema", "customer"), ais.getUserTable("schema",
                "order")), actions.hKey);
        Assert.assertEquals(Arrays.asList(ais.getUserTable("schema", "item")
                .getParentJoin()), actions.noHKey);
    }

    public void testCOIWithCascadingKeys() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false,
                false);
        builder.column("schema", "customer", "customer_name", 1, "varchar",
                64L, 0L, false, false);
        builder.index("schema", "customer", "customer_pk", true, "PRIMARY KEY");
        builder
                .indexColumn("schema", "customer", "customer_pk", "cid", 0,
                        true);
        builder.userTable("schema", "order");
        builder
                .column("schema", "order", "oid", 0, "int", 0L, 0L, false,
                        false);
        builder
                .column("schema", "order", "cid", 1, "int", 0L, 0L, false,
                        false);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L,
                false, false);
        builder.index("schema", "order", "order_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "order", "order_pk", "cid", 0, true);
        builder.indexColumn("schema", "order", "order_pk", "oid", 1, true);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "cid", 2, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "quantity", 3, "int", 0L, 0L, false,
                false);
        builder.index("schema", "item", "item_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "item", "item_pk", "cid", 0, true);
        builder.indexColumn("schema", "item", "item_pk", "oid", 1, true);
        builder.indexColumn("schema", "item", "item_pk", "iid", 2, true);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema",
                "order", "cid");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "order",
                "oid");
        builder.joinColumns("oi", "schema", "order", "cid", "schema", "order",
                "cid");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        TestActions actions = new TestActions();
        BulkLoader bulkLoader = new BulkLoader(ais, "group", "bulkload",
                actions);
        bulkLoader.run();
        Assert.assertEquals(Arrays.asList(ais
                .getUserTable("schema", "customer"), ais.getUserTable("schema",
                "order"), ais.getUserTable("schema", "item")), actions.hKey);
        Assert.assertTrue(actions.noHKey.isEmpty());
    }

    public void testCOIA() throws Exception {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false,
                false);
        builder.column("schema", "customer", "customer_name", 1, "varchar",
                64L, 0L, false, false);
        builder.index("schema", "customer", "customer_pk", true, "PRIMARY KEY");
        builder
                .indexColumn("schema", "customer", "customer_pk", "cid", 0,
                        true);
        builder.userTable("schema", "order");
        builder
                .column("schema", "order", "oid", 0, "int", 0L, 0L, false,
                        false);
        builder
                .column("schema", "order", "cid", 1, "int", 0L, 0L, false,
                        false);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L,
                false, false);
        builder.index("schema", "order", "order_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "order", "order_pk", "oid", 0, true);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false,
                false);
        builder.index("schema", "item", "item_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "item", "item_pk", "iid", 0, true);
        builder.userTable("schema", "address");
        builder.column("schema", "address", "aid", 0, "int", 0L, 0L, false,
                false);
        builder.column("schema", "address", "cid", 1, "int", 0L, 0L, false,
                false);
        builder.column("schema", "address", "line1", 2, "varchar", 100L, 0L,
                false, false);
        builder.column("schema", "address", "line2", 3, "varchar", 100L, 0L,
                false, false);
        builder.index("schema", "address", "address_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "address", "address_pk", "aid", 0, true);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema",
                "order", "cid");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "order",
                "oid");
        builder.joinTables("ca", "schema", "customer", "schema", "address");
        builder.joinColumns("ca", "schema", "customer", "cid", "schema",
                "address", "cid");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coia");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.addJoinToGroup("group", "ca", 0);
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        TestActions actions = new TestActions();
        BulkLoader bulkLoader = new BulkLoader(ais, "group", "bulkload",
                actions);
        bulkLoader.run();
        Assert.assertEquals(Arrays.asList(ais
                .getUserTable("schema", "customer"), ais.getUserTable("schema",
                "order"), ais.getUserTable("schema", "address")), actions.hKey);
        Assert.assertEquals(Arrays.asList(ais.getUserTable("schema", "item")
                .getParentJoin()), actions.noHKey);
    }

    public void testCOIX() throws Exception {
        // Table X is child of I. Ensures that "type 2" table works as parent.
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false,
                false);
        builder.column("schema", "customer", "customer_name", 1, "varchar",
                64L, 0L, false, false);
        builder.index("schema", "customer", "customer_pk", true, "PRIMARY KEY");
        builder
                .indexColumn("schema", "customer", "customer_pk", "cid", 0,
                        true);
        builder.userTable("schema", "order");
        builder
                .column("schema", "order", "oid", 0, "int", 0L, 0L, false,
                        false);
        builder
                .column("schema", "order", "cid", 1, "int", 0L, 0L, false,
                        false);
        builder.column("schema", "order", "order_date", 2, "int", 0L, 0L,
                false, false);
        builder.index("schema", "order", "order_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "order", "order_pk", "oid", 0, true);
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false);
        builder.column("schema", "item", "quantity", 2, "int", 0L, 0L, false,
                false);
        builder.index("schema", "item", "item_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "item", "item_pk", "iid", 0, true);
        builder.userTable("schema", "x");
        builder.column("schema", "x", "xid", 0, "int", 0L, 0L, false, false);
        builder.column("schema", "x", "iid", 1, "int", 0L, 0L, false, false);
        builder.column("schema", "x", "foobar", 2, "int", 0L, 0L, false, false);
        builder.index("schema", "x", "x_pk", true, "PRIMARY KEY");
        builder.indexColumn("schema", "x", "x_pk", "xid", 0, true);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema",
                "order", "cid");
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "order",
                "oid");
        builder.joinTables("ix", "schema", "item", "schema", "x");
        builder
                .joinColumns("ix", "schema", "item", "iid", "schema", "x",
                        "iid");
        builder.basicSchemaIsComplete();
        builder.createGroup("group", "groupschema", "coi");
        builder.addJoinToGroup("group", "co", 0);
        builder.addJoinToGroup("group", "oi", 0);
        builder.addJoinToGroup("group", "ix", 0);
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        TestActions actions = new TestActions();
        BulkLoader bulkLoader = new BulkLoader(ais, "group", "bulkload",
                actions);
        bulkLoader.run();
        Assert.assertEquals(Arrays.asList(ais
                .getUserTable("schema", "customer"), ais.getUserTable("schema",
                "order")), actions.hKey);
        Assert.assertEquals(Arrays.asList(ais.getUserTable("schema", "item")
                .getParentJoin(), ais.getUserTable("schema", "x")
                .getParentJoin()), actions.noHKey);
    }

    private static class TestActions implements TaskGenerator.Actions {
        @Override
        public void generateTasksForTableContainingHKeyColumns(
                BulkLoader loader, UserTable table,
                IdentityHashMap<UserTable, TableTasks> tasks) {
            hKey.add(table);
        }

        @Override
        public void generateTasksForTableNotContainingHKeyColumns(
                BulkLoader loader, Join join,
                IdentityHashMap<UserTable, TableTasks> tasks) {
            noHKey.add(join);
        }

        final List<UserTable> hKey = new ArrayList<UserTable>();
        final List<Join> noHKey = new ArrayList<Join>();
    }
}
