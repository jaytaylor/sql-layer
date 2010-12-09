package com.akiban.ais.model;

import org.junit.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static junit.framework.Assert.*;

public class AISTest
{
    @Test
    public void testTableColumnsReturnedInOrder()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col2", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col0", 0, "int", 0L, 0L, false, false, null, null);
        builder.basicSchemaIsComplete();
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        UserTable table = ais.getUserTable("schema", "table");
        int expectedPosition = 0;
        for (Column column : table.getColumns()) {
            assertEquals(expectedPosition, column.getPosition().intValue());
            expectedPosition++;
        }
    }

    @Test
    public void testIndexColumnsReturnedInOrder()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col2", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col3", 3, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col4", 4, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col5", 5, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "table", "index", false, "KEY");
        // Create index on (col5, col4, col3), adding index columns backwards
        builder.indexColumn("schema", "table", "index", "col3", 2, true, 0);
        builder.indexColumn("schema", "table", "index", "col4", 1, true, 0);
        builder.indexColumn("schema", "table", "index", "col5", 0, true, 0);
        builder.basicSchemaIsComplete();
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        UserTable table = ais.getUserTable("schema", "table");
        Index index = table.getIndex("index");
        Iterator<IndexColumn> indexColumnScan = index.getColumns().iterator();
        IndexColumn indexColumn = indexColumnScan.next();
        assertEquals(5, indexColumn.getColumn().getPosition().intValue());
        assertEquals(0, indexColumn.getPosition().intValue());
        indexColumn = indexColumnScan.next();
        assertEquals(4, indexColumn.getColumn().getPosition().intValue());
        assertEquals(1, indexColumn.getPosition().intValue());
        indexColumn = indexColumnScan.next();
        assertEquals(3, indexColumn.getColumn().getPosition().intValue());
        assertEquals(2, indexColumn.getPosition().intValue());
        assertTrue(!indexColumnScan.hasNext());
    }

    @Test
    public void testPKColumnsReturnedInOrder()
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("schema", "table");
        builder.column("schema", "table", "col0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col2", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col3", 3, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col4", 4, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "table", "col5", 5, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "table", "index", false, "PRIMARY");
        // Create index on (col5, col4, col3), adding index columns backwards
        builder.indexColumn("schema", "table", "index", "col3", 2, true, 0);
        builder.indexColumn("schema", "table", "index", "col4", 1, true, 0);
        builder.indexColumn("schema", "table", "index", "col5", 0, true, 0);
        builder.basicSchemaIsComplete();
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        UserTable table = ais.getUserTable("schema", "table");
        PrimaryKey pk = table.getPrimaryKey();
        Iterator<Column> indexColumnScan = pk.getColumns().iterator();
        Column pkColumn = indexColumnScan.next();
        assertEquals(5, pkColumn.getPosition().intValue());
        pkColumn = indexColumnScan.next();
        assertEquals(4, pkColumn.getPosition().intValue());
        pkColumn = indexColumnScan.next();
        assertEquals(3, pkColumn.getPosition().intValue());
        assertTrue(!indexColumnScan.hasNext());
    }

    @Test
    public void testJoinColumnsReturnedInOrder()
    {
        AISBuilder builder = new AISBuilder();
        // parent(p0, p1), pk is (p1, p0)
        builder.userTable("schema", "parent");
        builder.column("schema", "parent", "p0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "parent", "p1", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "parent", "pk", false, "PRIMARY");
        builder.indexColumn("schema", "parent", "pk", "p1", 0, true, 0);
        builder.indexColumn("schema", "parent", "pk", "p0", 1, true, 0);
        // child(c0, c1), fk to parent is (c0, c1). Add them backwards so we can make sure that join.getColumns()
        // fixes the ordering.
        builder.userTable("schema", "child");
        builder.column("schema", "child", "c0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "child", "c1", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "child", "pk", false, "PRIMARY");
        builder.indexColumn("schema", "child", "pk", "c0", 0, true, 0);
        builder.indexColumn("schema", "child", "pk", "c1", 1, true, 0);
        builder.joinTables("join", "schema", "parent", "schema", "child");
        builder.joinColumns("join", "schema", "parent", "p0", "schema", "child", "c1");
        builder.joinColumns("join", "schema", "parent", "p1", "schema", "child", "c0");
        builder.basicSchemaIsComplete();
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        Join join = ais.getJoin("join");
        Iterator<JoinColumn> joinColumns = join.getJoinColumns().iterator();
        JoinColumn joinColumn = joinColumns.next();
        assertEquals("p1", joinColumn.getParent().getName());
        assertEquals("c0", joinColumn.getChild().getName());
        joinColumn = joinColumns.next();
        assertEquals("p0", joinColumn.getParent().getName());
        assertEquals("c1", joinColumn.getChild().getName());
        assertTrue(!joinColumns.hasNext());
    }

    @Test
    public void testHKeyNonCascadingPKs()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "customer", "pk", "cid", 0, true, 0);
        // order(oid, cid) pk: oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "order", "pk", "oid", 0, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema", "order", "cid");
        // item(iid, oid) pk: iid, fk: oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "item", "pk", "iid", 0, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "item", "oid");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.groupingIsComplete();
        // get ready to test
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        // ---------------- Customer -------------------------------------
        UserTable customer = ais.getUserTable("schema", "customer");
        GroupTable coi = customer.getGroup().getGroupTable();
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("schema", "order");
        assertSame(coi, order.getGroup().getGroupTable());
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("schema", "item");
        assertSame(coi, item.getGroup().getGroupTable());
        checkHKey(item.hKey(),
                  customer, order, "cid",
                  order, item, "oid",
                  item, item, "iid");
        // ---------------- Branch hkeys -------------------------------------
        // customer
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid", "order$cid");
        // order
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid", "customer$cid");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid", "item$oid");
        // item
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "order$cid", "customer$cid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid", "order$oid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid");
    }

    @Test
    public void testHKeyNonCascadingMultiColumnPKs()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "cid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "customer", "pk", "cid0", 0, true, 0);
        builder.indexColumn("schema", "customer", "pk", "cid1", 1, true, 0);
        // order(oid, cid) pk: oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "oid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "oid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid0", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid1", 3, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "order", "pk", "oid0", 0, true, 0);
        builder.indexColumn("schema", "order", "pk", "oid1", 1, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid0", "schema", "order", "cid0");
        builder.joinColumns("co", "schema", "customer", "cid1", "schema", "order", "cid1");
        // item(iid, oid) pk: iid, fk: oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "iid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid0", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid1", 3, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "item", "pk", "iid0", 0, true, 0);
        builder.indexColumn("schema", "item", "pk", "iid1", 1, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid0", "schema", "item", "oid0");
        builder.joinColumns("oi", "schema", "order", "oid1", "schema", "item", "oid1");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.groupingIsComplete();
        // get ready to test
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        // ---------------- Customer -------------------------------------
        UserTable customer = ais.getUserTable("schema", "customer");
        GroupTable coi = customer.getGroup().getGroupTable();
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("schema", "order");
        assertSame(coi, order.getGroup().getGroupTable());
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("schema", "item");
        assertSame(coi, item.getGroup().getGroupTable());
        checkHKey(item.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, item, "oid0", item, "oid1",
                  item, item, "iid0", item, "iid1");
        // ---------------- Branch hkeys -------------------------------------
        // customer
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid0", "order$cid0");
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid1", "order$cid1");
        // order
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid0", "customer$cid0");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid1", "customer$cid1");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid0", "item$oid0");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid1", "item$oid1");
        // item
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "order$cid0", "customer$cid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "order$cid1", "customer$cid1");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid0", "order$oid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid1", "order$oid1");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid1");
    }

    @Test
    public void testHKeyCascadingPKs()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "customer", "pk", "cid", 0, true, 0);
        // order(cid, oid) pk: cid, oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "order", "pk", "cid", 0, true, 0);
        builder.indexColumn("schema", "order", "pk", "oid", 1, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema", "order", "cid");
        // item(cid, oid, iid) pk: cid, oid, iid, fk: cid, oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "iid", 2, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "item", "pk", "cid", 0, true, 0);
        builder.indexColumn("schema", "item", "pk", "oid", 1, true, 0);
        builder.indexColumn("schema", "item", "pk", "iid", 2, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "cid", "schema", "item", "cid");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "item", "oid");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.groupingIsComplete();
        // get ready to test
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        // ---------------- Customer -------------------------------------
        UserTable customer = ais.getUserTable("schema", "customer");
        GroupTable coi = customer.getGroup().getGroupTable();
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("schema", "order");
        assertSame(coi, order.getGroup().getGroupTable());
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("schema", "item");
        assertSame(coi, item.getGroup().getGroupTable());
        checkHKey(item.hKey(),
                  customer, item, "cid",
                  order, item, "oid",
                  item, item, "iid");
        // ---------------- Branch hkeys -------------------------------------
        // customer
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid", "order$cid", "item$cid");
        // order
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid", "customer$cid", "item$cid");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid", "item$oid");
        // item
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "item$cid", "customer$cid", "order$cid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid", "order$oid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid");
    }

    @Test
    public void testHKeyCascadingMultiColumnPKs()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "customer", "cid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "customer", "pk", "cid0", 0, true, 0);
        builder.indexColumn("schema", "customer", "pk", "cid1", 1, true, 0);
        // order(cid, oid) pk: cid, oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "cid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "oid0", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "oid1", 3, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "order", "pk", "cid0", 0, true, 0);
        builder.indexColumn("schema", "order", "pk", "cid1", 1, true, 0);
        builder.indexColumn("schema", "order", "pk", "oid0", 2, true, 0);
        builder.indexColumn("schema", "order", "pk", "oid1", 3, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid0", "schema", "order", "cid0");
        builder.joinColumns("co", "schema", "customer", "cid1", "schema", "order", "cid1");
        // item(cid, oid, iid) pk: cid, oid, iid, fk: cid, oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "cid0", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "cid1", 1, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid0", 2, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid1", 3, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "iid0", 4, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "iid1", 5, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "item", "pk", "cid0", 0, true, 0);
        builder.indexColumn("schema", "item", "pk", "cid1", 1, true, 0);
        builder.indexColumn("schema", "item", "pk", "oid0", 2, true, 0);
        builder.indexColumn("schema", "item", "pk", "oid1", 3, true, 0);
        builder.indexColumn("schema", "item", "pk", "iid0", 4, true, 0);
        builder.indexColumn("schema", "item", "pk", "iid1", 5, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "cid0", "schema", "item", "cid0");
        builder.joinColumns("oi", "schema", "order", "cid1", "schema", "item", "cid1");
        builder.joinColumns("oi", "schema", "order", "oid0", "schema", "item", "oid0");
        builder.joinColumns("oi", "schema", "order", "oid1", "schema", "item", "oid1");
        builder.basicSchemaIsComplete();
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.groupingIsComplete();
        // get ready to test
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        // ---------------- Customer -------------------------------------
        UserTable customer = ais.getUserTable("schema", "customer");
        GroupTable coi = customer.getGroup().getGroupTable();
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("schema", "order");
        assertSame(coi, order.getGroup().getGroupTable());
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("schema", "item");
        assertSame(coi, item.getGroup().getGroupTable());
        checkHKey(item.hKey(),
                  customer, item, "cid0", item, "cid1",
                  order, item, "oid0", item, "oid1",
                  item, item, "iid0", item, "iid1");
        // ---------------- Branch hkeys -------------------------------------
        // customer
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid0", "order$cid0", "item$cid0");
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid1", "order$cid1", "item$cid1");
        // order
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid0", "customer$cid0", "item$cid0");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid1", "customer$cid1", "item$cid1");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid0", "item$oid0");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid1", "item$oid1");
        // item
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "item$cid0", "customer$cid0", "order$cid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "item$cid1", "customer$cid1", "order$cid1");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid0", "order$oid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid1", "order$oid1");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid0");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid1");
    }

    @Test
    public void testHKeyWithBranches()
    {
        AISBuilder builder = new AISBuilder();
        // customer(cid) pk: cid
        builder.userTable("schema", "customer");
        builder.column("schema", "customer", "cid", 0, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "customer", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "customer", "pk", "cid", 0, true, 0);
        // order(oid, cid) pk: oid, fk: cid
        builder.userTable("schema", "order");
        builder.column("schema", "order", "oid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "order", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "order", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "order", "pk", "oid", 0, true, 0);
        builder.joinTables("co", "schema", "customer", "schema", "order");
        builder.joinColumns("co", "schema", "customer", "cid", "schema", "order", "cid");
        // item(iid, oid) pk: iid, fk: oid
        builder.userTable("schema", "item");
        builder.column("schema", "item", "iid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "item", "oid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "item", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "item", "pk", "iid", 0, true, 0);
        builder.joinTables("oi", "schema", "order", "schema", "item");
        builder.joinColumns("oi", "schema", "order", "oid", "schema", "item", "oid");
        builder.basicSchemaIsComplete();
        // address(aid, cid) pk: aid, fk: cid
        builder.userTable("schema", "address");
        builder.column("schema", "address", "aid", 0, "int", 0L, 0L, false, false, null, null);
        builder.column("schema", "address", "cid", 1, "int", 0L, 0L, false, false, null, null);
        builder.index("schema", "address", "pk", true, "PRIMARY");
        builder.indexColumn("schema", "address", "pk", "aid", 0, true, 0);
        builder.joinTables("ca", "schema", "customer", "schema", "address");
        builder.joinColumns("ca", "schema", "customer", "cid", "schema", "address", "cid");
        // Create group
        builder.createGroup("coi", "coi", "coi");
        builder.addJoinToGroup("coi", "co", 0);
        builder.addJoinToGroup("coi", "oi", 0);
        builder.addJoinToGroup("coi", "ca", 0);
        builder.groupingIsComplete();
        // get ready to test
        AkibaInformationSchema ais = builder.akibaInformationSchema();
        // ---------------- Customer -------------------------------------
        UserTable customer = ais.getUserTable("schema", "customer");
        GroupTable coi = customer.getGroup().getGroupTable();
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("schema", "order");
        assertSame(coi, order.getGroup().getGroupTable());
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("schema", "item");
        assertSame(coi, item.getGroup().getGroupTable());
        checkHKey(item.hKey(),
                  customer, order, "cid",
                  order, item, "oid",
                  item, item, "iid");
        // ---------------- Item -------------------------------------
        UserTable address = ais.getUserTable("schema", "address");
        assertSame(coi, address.getGroup().getGroupTable());
        checkHKey(address.hKey(),
                  customer, address, "cid",
                  address, address, "aid");
        // ---------------- Branch hkeys -------------------------------------
        // customer
        checkBranchHKeyColumn(customer.branchHKey(), coi,
                              customer, "customer$cid", "order$cid");
        // order
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              customer, "order$cid", "customer$cid");
        checkBranchHKeyColumn(order.branchHKey(), coi,
                              order, "order$oid", "item$oid");
        // item
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              customer, "order$cid", "customer$cid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              order, "item$oid", "order$oid");
        checkBranchHKeyColumn(item.branchHKey(), coi,
                              item, "item$iid");
        // address
        checkBranchHKeyColumn(address.branchHKey(), coi,
                              customer, "address$cid", "customer$cid");
        checkBranchHKeyColumn(address.branchHKey(), coi,
                              address, "address$aid");
    }

    private void checkHKey(HKey hKey, Object ... elements)
    {
        int e = 0;
        int position = 0;
        for (HKeySegment segment : hKey.segments()) {
            assertEquals(position++, segment.positionInHKey());
            assertSame(elements[e++], segment.table());
            for (HKeyColumn column : segment.columns()) {
                assertEquals(position++, column.positionInHKey());
                assertEquals(elements[e++], column.column().getTable());
                assertEquals(elements[e++], column.column().getName());
            }
        }
        assertEquals(elements.length, e);
    }

    private void checkBranchHKeyColumn(HKey hKey, GroupTable groupTable,
                                       UserTable segmentUserTable,
                                       String columnName,
                                       Object ... matches)
    {
        HKeySegment segment = null;
        for (HKeySegment s : hKey.segments()) {
            if (s.table() == segmentUserTable) {
                segment = s;
            }
        }
        assertNotNull(segment);
        HKeyColumn column = null;
        for (HKeyColumn c : segment.columns()) {
            if (c.column().getName().equals(columnName)) {
                column = c;
            }
        }
        assertNotNull(column);
        assertNotNull(column.equivalentColumns());
        Set<String> expected = new HashSet<String>();
        for (Column equivalentColumn : column.equivalentColumns()) {
            assertSame(groupTable, equivalentColumn.getTable());
            expected.add(equivalentColumn.getName());
        }
        Set<String> actual = new HashSet<String>();
        actual.add(columnName);
        for (Object m : matches) {
            actual.add((String) m);
        }
        assertEquals(expected, actual);
    }
}
