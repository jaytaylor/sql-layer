package com.akiban.ais.model;

import junit.framework.Assert;
import org.junit.Test;

import java.util.Iterator;

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
            Assert.assertEquals(expectedPosition, column.getPosition().intValue());
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
        Assert.assertEquals(5, indexColumn.getColumn().getPosition().intValue());
        Assert.assertEquals(0, indexColumn.getPosition().intValue());
        indexColumn = indexColumnScan.next();
        Assert.assertEquals(4, indexColumn.getColumn().getPosition().intValue());
        Assert.assertEquals(1, indexColumn.getPosition().intValue());
        indexColumn = indexColumnScan.next();
        Assert.assertEquals(3, indexColumn.getColumn().getPosition().intValue());
        Assert.assertEquals(2, indexColumn.getPosition().intValue());
        Assert.assertTrue(!indexColumnScan.hasNext());
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
        Assert.assertEquals(5, pkColumn.getPosition().intValue());
        pkColumn = indexColumnScan.next();
        Assert.assertEquals(4, pkColumn.getPosition().intValue());
        pkColumn = indexColumnScan.next();
        Assert.assertEquals(3, pkColumn.getPosition().intValue());
        Assert.assertTrue(!indexColumnScan.hasNext());
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
        Assert.assertEquals("p1", joinColumn.getParent().getName());
        Assert.assertEquals("c0", joinColumn.getChild().getName());
        joinColumn = joinColumns.next();
        Assert.assertEquals("p0", joinColumn.getParent().getName());
        Assert.assertEquals("c1", joinColumn.getChild().getName());
        Assert.assertTrue(!joinColumns.hasNext());
    }
    
    @Test public void testHKeyNonCascadingPKs()
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
        Iterator<Column> allHKeyColumns;
        Iterator<Column> localHKeyColumns;
        Column column;
        // Check customer hkey
        UserTable customer = ais.getUserTable("schema", "customer");
        allHKeyColumns = customer.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = customer.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check order hkey
        UserTable order = ais.getUserTable("schema", "order");
        allHKeyColumns = order.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = order.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check item hkey
        UserTable item = ais.getUserTable("schema", "item");
        allHKeyColumns = item.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = item.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
    }

    @Test public void testHKeyNonCascadingMultiColumnPKs()
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
        Iterator<Column> allHKeyColumns;
        Iterator<Column> localHKeyColumns;
        Column column;
        // Check customer hkey
        UserTable customer = ais.getUserTable("schema", "customer");
        allHKeyColumns = customer.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = customer.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check order hkey
        UserTable order = ais.getUserTable("schema", "order");
        allHKeyColumns = order.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = order.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check item hkey
        UserTable item = ais.getUserTable("schema", "item");
        allHKeyColumns = item.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = item.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
    }

    @Test public void testHKeyCascadingPKs()
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
        Iterator<Column> allHKeyColumns;
        Iterator<Column> localHKeyColumns;
        Column column;
        // Check customer hkey
        UserTable customer = ais.getUserTable("schema", "customer");
        allHKeyColumns = customer.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = customer.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check order hkey
        UserTable order = ais.getUserTable("schema", "order");
        allHKeyColumns = order.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = order.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check item hkey
        UserTable item = ais.getUserTable("schema", "item");
        allHKeyColumns = item.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = item.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
    }

    @Test public void testHKeyCascadingMultiColumnPKs()
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
        Iterator<Column> allHKeyColumns;
        Iterator<Column> localHKeyColumns;
        Column column;
        // Check customer hkey
        UserTable customer = ais.getUserTable("schema", "customer");
        allHKeyColumns = customer.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = customer.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(customer, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check order hkey
        UserTable order = ais.getUserTable("schema", "order");
        allHKeyColumns = order.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = order.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(order, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
        // Check item hkey
        UserTable item = ais.getUserTable("schema", "item");
        allHKeyColumns = item.allHKeyColumns().iterator();
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid0", column.getName());
        column = allHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid1", column.getName());
        Assert.assertTrue(!allHKeyColumns.hasNext());
        localHKeyColumns = item.localHKeyColumns().iterator();
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("cid1", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("oid1", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid0", column.getName());
        column = localHKeyColumns.next();
        Assert.assertEquals(item, column.getTable());
        Assert.assertEquals("iid1", column.getName());
        Assert.assertTrue(!localHKeyColumns.hasNext());
    }
}
