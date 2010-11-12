package com.akiban.cserver;

import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class RowDefCacheTest
{
    @Test
    public void testMultipleBadlyOrderedColumns() throws Exception
    {
        String[] ddl = {
            String.format("use `%s`; ", SCHEMA),
            "create table b(",
            "    b0 int,",
            "    b1 int,",
            "    b2 int,",
            "    b3 int,",
            "    b4 int,",
            "    b5 int,",
            "    primary key(b3, b2, b4, b1)",
            ") engine = akibandb;",
            "create table bb(",
            "    bb0 int,",
            "    bb1 int,",
            "    bb2 int,",
            "    bb3 int,",
            "    bb4 int,",
            "    bb5 int,",
            "    primary key (bb0, bb5, bb3, bb2, bb4), ",
            "CONSTRAINT `__akiban_fk_0` FOREIGN KEY `__akiban_fk_0` (`bb0`,`bb2`,`bb1`,`bb3`) REFERENCES `b` (`b3`,`b2`,`b4`,`b1`)",
            ") engine = akibandb;",
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        RowDef b = rowDefCache.getRowDef(tableName("b"));
        RowDef bb = rowDefCache.getRowDef(tableName("bb"));
        assertArrayEquals(new int[]{3, 2, 4, 1}, b.getPkFields());
        assertArrayEquals(new int[]{}, b.getParentJoinFields());
        assertArrayEquals(new int[]{5, 4}, bb.getPkFields());
        assertArrayEquals(new int[]{0, 2, 1, 3}, bb.getParentJoinFields());
    }

    @Test
    public void childDoesNotContributeToHKey() throws Exception
    {
        String[] ddl = {
            String.format("use `%s`;", SCHEMA),
            "create table parent (",
            "   id int,",
            "   primary key(id)",
            ") engine = akibandb;",
            "create table child (",
            "   id int,",
            "   primary key(id),",
            "   constraint `__akiban_fk0` foreign key `akibafk` (id) references parent(id)",
            ") engine = akibandb;"
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        RowDef parent = rowDefCache.getRowDef(tableName("parent"));
        RowDef child = rowDefCache.getRowDef(tableName("child"));
        assertArrayEquals("parent PKs", new int[]{0}, parent.getPkFields());
        assertArrayEquals("parent joins", new int[]{}, parent.getParentJoinFields());
        assertArrayEquals("child PKs", new int[]{}, child.getPkFields());
        assertArrayEquals("child joins", new int[]{0}, child.getParentJoinFields());
    }

    @Test
    public void testNonCascadingPKs() throws Exception
    {
        String[] ddl = {
            String.format("use %s; ", SCHEMA),
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid)",
            ") engine = akibandb; "  ,
            "create table orders(",
            "    oid int not null, ",
            "    cid int not null, ",
            "    ox int not null, ",
            "    primary key(oid) ",
            "    , constraint __akiban_oc foreign key co(cid) references customer(cid)",
            ") engine = akibandb; ",
            "create table item(",
            "    iid int not null, ",
            "    oid int not null, ",
            "    ix int not null, ",
            "    primary key(iid) ",
            "    , constraint __akiban_io foreign key io(oid) references orders(oid)",
            ") engine = akibandb; "
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        // Customer
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "customer.cid", "customer.cx");
        assertArrayEquals(new int[]{0}, customer.getPkFields());
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // Orders
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "orders.oid", "orders.cid", "orders.ox");
        assertArrayEquals(new int[]{0}, orders.getPkFields());
        assertArrayEquals(new int[]{1}, orders.getParentJoinFields());
        // Item
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "item.iid", "item.oid", "item.ix");
        assertArrayEquals(new int[]{0}, item.getPkFields());
        assertArrayEquals(new int[]{1}, item.getParentJoinFields());
    }

    @Test
    public void testCascadingPKs() throws Exception
    {
        String[] ddl = {
            String.format("use %s; ", SCHEMA),
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid)",
            ") engine = akibandb; "  ,
            "create table orders(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    ox int not null, ",
            "    primary key(cid, oid) ",
            "    , constraint __akiban_oc foreign key co(cid) references customer(cid)",
            ") engine = akibandb; ",
            "create table item(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    iid int not null, ",
            "    ix int not null, ",
            "    primary key(cid, oid, iid) ",
            "    , constraint __akiban_io foreign key io(cid, oid) references orders(cid, oid)",
            ") engine = akibandb; "
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        // Customer
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "customer.cid", "customer.cx");
        assertArrayEquals(new int[]{0}, customer.getPkFields());
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // Orders
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "orders.cid", "orders.oid", "orders.ox");
        assertArrayEquals(new int[]{1}, orders.getPkFields());
        assertArrayEquals(new int[]{0}, orders.getParentJoinFields());
        // Item
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "item.cid", "item.oid", "item.iid", "item.ix");
        assertArrayEquals(new int[]{2}, item.getPkFields());
        assertArrayEquals(new int[]{0, 1}, item.getParentJoinFields());
    }

    private void checkFields(RowDef rowdef, String ... expectedFields)
    {
        FieldDef[] fields = rowdef.getFieldDefs();
        Assert.assertEquals(expectedFields.length, fields.length);
        for (int i = 0; i < fields.length; i++) {
            String actualName = String.format("%s.%s", rowdef.getTableName(), fields[i].getName());
            assertEquals(expectedFields[i], actualName);
        }
    }

    private String tableName(String name)
    {
        return String.format("%s.%s", SCHEMA, name);
    }

    private static final String SCHEMA = "row_def_cache_test";
    private static final RowDefCacheFactory ROW_DEF_CACHE_FACTORY = new RowDefCacheFactory();
}
