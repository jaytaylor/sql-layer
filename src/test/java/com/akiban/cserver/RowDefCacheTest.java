package com.akiban.cserver;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.junit.Test;

import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.UserTable;

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
        UserTable bTable = b.userTable();
        checkHKey(bTable.hKey(), bTable, bTable, "b3", bTable, "b2", bTable, "b4", bTable, "b1");
        assertEquals(5, b.getHKeyDepth()); // b ordinal, b3, b2, b4, b1
        RowDef bb = rowDefCache.getRowDef(tableName("bb"));
        UserTable bbTable = bb.userTable();
        checkHKey(bbTable.hKey(),
                  bTable, bbTable, "bb0", bbTable, "bb2", bbTable, "bb1", bbTable, "bb3",
                  bbTable, bbTable, "bb5", bbTable, "bb4");
        assertEquals(8, bb.getHKeyDepth()); // b ordinal, b3, b2, b4, b1, bb ordinal, b5, b4
        assertEquals(6, b.getFieldDefs().length);
        checkField("b0", b, 0);
        checkField("b1", b, 1);
        checkField("b2", b, 2);
        checkField("b3", b, 3);
        checkField("b4", b, 4);
        checkField("b5", b, 5);
        assertEquals(6, bb.getFieldDefs().length);
        checkField("bb0", bb, 0);
        checkField("bb1", bb, 1);
        checkField("bb2", bb, 2);
        checkField("bb3", bb, 3);
        checkField("bb4", bb, 4);
        checkField("bb5", bb, 5);
        assertArrayEquals(new int[]{3, 2, 4, 1}, b.getPkFields());
        assertArrayEquals(new int[]{}, b.getParentJoinFields());
        assertArrayEquals(new int[]{5, 4}, bb.getPkFields());
        assertArrayEquals(new int[]{0, 2, 1, 3}, bb.getParentJoinFields());
        assertEquals(b.getRowDefId(), bb.getParentRowDefId());
        assertEquals(0, b.getParentRowDefId());
        RowDef group = rowDefCache.getRowDef(b.getGroupRowDefId());
        checkField("b$b0", group, 0);
        checkField("b$b1", group, 1);
        checkField("b$b2", group, 2);
        checkField("b$b3", group, 3);
        checkField("b$b4", group, 4);
        checkField("b$b5", group, 5);
        checkField("bb$bb0", group, 6);
        checkField("bb$bb1", group, 7);
        checkField("bb$bb2", group, 8);
        checkField("bb$bb3", group, 9);
        checkField("bb$bb4", group, 10);
        checkField("bb$bb5", group, 11);
        assertEquals(group.getRowDefId(), b.getGroupRowDefId());
        assertEquals(group.getRowDefId(), bb.getGroupRowDefId());
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
        UserTable p = parent.userTable();
        checkHKey(p.hKey(), p, p, "id");
        assertEquals(2, parent.getHKeyDepth()); // parent ordinal, id
        RowDef child = rowDefCache.getRowDef(tableName("child"));
        UserTable c = child.userTable();
        checkHKey(c.hKey(),
                  p, c, "id",
                  c);
        assertEquals(3, child.getHKeyDepth()); // parent ordinal, id, child ordinal
        assertArrayEquals("parent PKs", new int[]{0}, parent.getPkFields());
        assertArrayEquals("parent joins", new int[]{}, parent.getParentJoinFields());
        assertArrayEquals("child PKs", new int[]{}, child.getPkFields());
        assertArrayEquals("child joins", new int[]{0}, child.getParentJoinFields());
    }

    @Test
    public void testUserIndexDefs() throws Exception
    {
        String[] ddl = {
            String.format("use %s;", SCHEMA),
            "create table t (",
            "    a int, ",
            "    b int, ",
            "    c int, ",
            "    d int, ",
            "    e int, ",
            "    primary key(c, a), ",
            "    key(e, d), ",
            "    unique key(d, b)",
            ");"
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        RowDef t = rowDefCache.getRowDef(tableName("t"));
        assertEquals(3, t.getHKeyDepth()); // t ordinal, c, a
        IndexDef index;
        index = index(t, "c", "a");
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        index = index(t, "e", "d");
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        index = index(t, "d", "b");
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
    }

    @Test
    public void testNonCascadingPKs() throws Exception
    {
        String[] ddl = {
            String.format("use %s; ", SCHEMA),
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid), ",
            "    unique key(cid, cx) ",
            ") engine = akibandb; ",
            "create table orders(",
            "    oid int not null, ",
            "    cid int not null, ",
            "    ox int not null, ",
            "    primary key(oid), ",
            "    unique key(cid, oid), ",
            "    unique key(oid, cid), ",
            "    unique key(cid, oid, ox), ",
            "    constraint __akiban_oc foreign key co(cid) references customer(cid)",
            ") engine = akibandb; ",
            "create table item(",
            "    iid int not null, ",
            "    oid int not null, ",
            "    ix int not null, ",
            "    primary key(iid), ",
            "    key(oid, iid), ",
            "    key(iid, oid), ",
            "    key(oid, iid, ix), ",
            "    constraint __akiban_io foreign key io(oid) references orders(oid)",
            ") engine = akibandb; "
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        IndexDef index;
        int[] fields;
        IndexDef.H2I[] indexKeyFields;
        IndexDef.I2H[] hKeyFields;
        // ------------------------- Customer ------------------------------------------
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "cid", "cx");
        assertEquals(2, customer.getHKeyDepth()); // customer ordinal, cid
        assertArrayEquals(new int[]{0}, customer.getPkFields());
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // index on cid
        index = index(customer, "cid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // c.cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // c.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // index on cid, cx
        index = index(customer, "cid", "cx");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // c.cid
        assertEquals(1, fields[1]); // c.cx
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // c.cid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // c.cx
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // ------------------------- Orders ------------------------------------------
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "oid", "cid", "ox");
        assertEquals(4, orders.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid
        assertArrayEquals(new int[]{0}, orders.getPkFields());
        assertArrayEquals(new int[]{1}, orders.getParentJoinFields());
        // index on oid
        index = index(orders, "oid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // o.oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // o.oid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // o.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        // index on cid, oid
        index = index(orders, "cid", "oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // o.cid
        assertEquals(0, fields[1]); // o.oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // o.cid
        assertEquals(0, indexKeyFields[1].fieldIndex()); // o.oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc());
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // index on oid, cid
        index = index(orders, "oid", "cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // o.oid
        assertEquals(1, fields[1]); // o.cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // o.oid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // o.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        // index on cid, oid, ox
        index = index(orders, "cid", "oid", "ox");
        assertNotNull(index);
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // o.cid
        assertEquals(0, fields[1]); // o.oid
        assertEquals(2, fields[2]); // o.ox
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // o.cid
        assertEquals(0, indexKeyFields[1].fieldIndex()); // o.oid
        assertEquals(2, indexKeyFields[2].fieldIndex()); // o.ox
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // ------------------------- Item ------------------------------------------
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "iid", "oid", "ix");
        assertEquals(6, item.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid, item ordinal, iid
        assertArrayEquals(new int[]{0}, item.getPkFields());
        assertArrayEquals(new int[]{1}, item.getParentJoinFields());
        // Index on iid
        index = index(item, "iid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // i.iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // i.iid
        assertEquals(1, indexKeyFields[1].hKeyLoc()); // hkey cid
        assertEquals(1, indexKeyFields[2].fieldIndex()); // i.oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(0, hKeyFields[5].indexKeyLoc()); // index oid
        // Index on oid, iid
        index = index(item, "oid", "iid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // i.oid
        assertEquals(0, fields[1]); // i.iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // i.oid
        assertEquals(0, indexKeyFields[1].fieldIndex()); // i.iid
        assertEquals(1, indexKeyFields[2].hKeyLoc()); // hkey cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index iid
        // Index on iid, oid
        index = index(item, "iid", "oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // i.iid
        assertEquals(1, fields[1]); // i.oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // i.iid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // i.oid
        assertEquals(1, indexKeyFields[2].hKeyLoc()); // hkey cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(0, hKeyFields[5].indexKeyLoc()); // index iid
        // Index on oid, iid, ix
        index = index(item, "oid", "iid", "ix");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // i.oid
        assertEquals(0, fields[1]); // i.iid
        assertEquals(2, fields[2]); // i.ix
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // i.oid
        assertEquals(0, indexKeyFields[1].fieldIndex()); // i.iid
        assertEquals(2, indexKeyFields[2].fieldIndex()); // i.ix
        assertEquals(1, indexKeyFields[3].hKeyLoc()); // hkey cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(3, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index iid
        // ------------------------- COI ------------------------------------------
        RowDef coi = rowDefCache.getRowDef(customer.getGroupRowDefId());
        checkFields(coi,
                    "customer$cid", "customer$cx",
                    "orders$oid", "orders$cid", "orders$ox",
                    "item$iid", "item$oid", "item$ix");
        assertArrayEquals(new RowDef[]{customer, orders, item}, coi.getUserTableRowDefs());
        // PK index on customer
        index = index(coi, "customer$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // customer$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // customer$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // PK index on order
        index = index(coi, "orders$oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(2, fields[0]); // orders$oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(2, indexKeyFields[0].fieldIndex()); // orders$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        // PK index on item
        index = index(coi, "item$iid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(5, fields[0]); // item$iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(5, indexKeyFields[0].fieldIndex()); // item$iid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(0, hKeyFields[5].indexKeyLoc()); // index oid
        // FK index on orders.cid
        index = index(coi, "orders$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(3, fields[0]); // orders$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(3, indexKeyFields[0].fieldIndex()); // orders$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // FK index on item.oid
        index = index(coi, "item$oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(6, fields[0]); // item$oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(6, indexKeyFields[0].fieldIndex()); // item$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(2, hKeyFields[5].indexKeyLoc()); // index iid
        // index on customer cid, cx
        index = index(coi, "customer$cid", "customer$cx");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // customer$cid
        assertEquals(1, fields[1]); // customer$cx
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // customer$cid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // customer$cx
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // index on orders cid, oid
        index = index(coi, "orders$cid", "orders$oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(3, fields[0]); // orders$cid
        assertEquals(2, fields[1]); // orders$oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(3, indexKeyFields[0].fieldIndex()); // orders$cid
        assertEquals(2, indexKeyFields[1].fieldIndex()); // orders$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // index on orders oid, cid
        index = index(coi, "orders$oid", "orders$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(2, fields[0]); // orders$oid
        assertEquals(3, fields[1]); // orders$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(2, indexKeyFields[0].fieldIndex()); // orders$oid
        assertEquals(3, indexKeyFields[1].fieldIndex()); // orders$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        // index on orders cid, oid, ox
        index = index(coi, "orders$cid", "orders$oid", "orders$ox");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(3, fields[0]); // orders$cid
        assertEquals(2, fields[1]); // orders$oid
        assertEquals(4, fields[2]); // orders$ox
        indexKeyFields = index.indexKeyFields();
        assertEquals(3, indexKeyFields[0].fieldIndex()); // orders$cid
        assertEquals(2, indexKeyFields[1].fieldIndex()); // orders$oid
        assertEquals(4, indexKeyFields[2].fieldIndex()); // orders$ox
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // index on item oid, iid
        index = index(coi, "item$oid", "item$iid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(6, fields[0]); // item$oid
        assertEquals(5, fields[1]); // item$iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(6, indexKeyFields[0].fieldIndex()); // item$oid
        assertEquals(5, indexKeyFields[1].fieldIndex()); // item$iid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index iid
        // index on item iid, oid
        index = index(coi, "item$iid", "item$oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(5, fields[0]); // item$iid
        assertEquals(6, fields[1]); // item$oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(5, indexKeyFields[0].fieldIndex()); // item$iid
        assertEquals(6, indexKeyFields[1].fieldIndex()); // item$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(0, hKeyFields[5].indexKeyLoc()); // index iid
        // index on item oid, iid, ix
        index = index(coi, "item$oid", "item$iid", "item$ix");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(6, fields[0]); // item$oid
        assertEquals(5, fields[1]); // item$iid
        assertEquals(7, fields[2]); // item$ix
        indexKeyFields = index.indexKeyFields();
        assertEquals(6, indexKeyFields[0].fieldIndex()); // item$oid
        assertEquals(5, indexKeyFields[1].fieldIndex()); // item$iid
        assertEquals(7, indexKeyFields[2].fieldIndex()); // item$ix
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(3, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(0, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index iid
    }

    @Test
    public void testCascadingPKs() throws Exception
    {
        String[] ddl = {
            String.format("use %s; ", SCHEMA),
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid), ",
            "    key(cx)",
            ") engine = akibandb; ",
            "create table orders(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    ox int not null, ",
            "    primary key(cid, oid), ",
            "    key(ox, cid), ",
            "    constraint __akiban_oc foreign key co(cid) references customer(cid)",
            ") engine = akibandb; ",
            "create table item(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    iid int not null, ",
            "    ix int not null, ",
            "    primary key(cid, oid, iid), ",
            "    key(ix, iid, oid, cid), ",
            "    constraint __akiban_io foreign key io(cid, oid) references orders(cid, oid)",
            ") engine = akibandb; "
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        IndexDef index;
        int[] fields;
        IndexDef.H2I[] indexKeyFields;
        IndexDef.I2H[] hKeyFields;
        // ------------------------- Customer ------------------------------------------
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "cid", "cx");
        assertEquals(2, customer.getHKeyDepth()); // customer ordinal, cid
        assertArrayEquals(new int[]{0}, customer.getPkFields());
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // index on cid
        index = index(customer, "cid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // c.cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // c.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // index on cx
        index = index(customer, "cx");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // c.cx
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // c.cx
        assertEquals(0, indexKeyFields[1].fieldIndex()); // c.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        // ------------------------- Orders ------------------------------------------
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "cid", "oid", "ox");
        assertEquals(4, orders.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid
        assertArrayEquals(new int[]{1}, orders.getPkFields());
        assertArrayEquals(new int[]{0}, orders.getParentJoinFields());
        // index on cid, oid
        index = index(orders, "cid", "oid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // o.cid
        assertEquals(1, fields[1]); // o.oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // o.cid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // o.oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // index on ox, cid
        index = index(orders, "ox", "cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(2, fields[0]); // o.ox
        assertEquals(0, fields[1]); // o.cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(2, indexKeyFields[0].fieldIndex()); // o.ox
        assertEquals(0, indexKeyFields[1].fieldIndex()); // o.cid
        assertEquals(1, indexKeyFields[2].fieldIndex()); // o.oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        // ------------------------- Item ------------------------------------------
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "cid", "oid", "iid", "ix");
        assertEquals(6, item.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid, item ordinal iid
        assertArrayEquals(new int[]{2}, item.getPkFields());
        assertArrayEquals(new int[]{0, 1}, item.getParentJoinFields());
        // index on cid, oid, iid
        index = index(item, "cid", "oid", "iid");
        assertNotNull(index);
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // i.cid
        assertEquals(1, fields[1]); // i.oid
        assertEquals(2, fields[2]); // i.iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // i.cid
        assertEquals(1, indexKeyFields[1].fieldIndex()); // i.oid
        assertEquals(2, indexKeyFields[2].fieldIndex()); // i.iid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(2, hKeyFields[5].indexKeyLoc()); // index oid
        // index on ix, iid, oid, cid
        index = index(item, "ix", "iid", "oid", "cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(3, fields[0]); // i.ix
        assertEquals(2, fields[1]); // i.iid
        assertEquals(1, fields[2]); // i.oid
        assertEquals(0, fields[3]); // i.cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(3, indexKeyFields[0].fieldIndex()); // i.ix
        assertEquals(2, indexKeyFields[1].fieldIndex()); // i.iid
        assertEquals(1, indexKeyFields[2].fieldIndex()); // i.oid
        assertEquals(0, indexKeyFields[3].fieldIndex()); // i.cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(3, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index oid
        // ------------------------- COI ------------------------------------------
        RowDef coi = rowDefCache.getRowDef(customer.getGroupRowDefId());
        checkFields(coi,
                    "customer$cid", "customer$cx",
                    "orders$cid", "orders$oid", "orders$ox",
                    "item$cid", "item$oid", "item$iid", "item$ix");
        assertArrayEquals(new RowDef[]{customer, orders, item}, coi.getUserTableRowDefs());
        // customer PK index
        index = index(coi, "customer$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(0, fields[0]); // customer$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // customer$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        // orders PK index
        index = index(coi, "orders$cid", "orders$oid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(2, fields[0]); // orders$cid
        assertEquals(3, fields[1]); // orders$oid
        indexKeyFields = index.indexKeyFields();
        assertEquals(2, indexKeyFields[0].fieldIndex()); // orders$cid
        assertEquals(3, indexKeyFields[1].fieldIndex()); // orders$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // item PK index
        index = index(coi, "item$cid", "item$oid", "item$iid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(5, fields[0]); // item$cid
        assertEquals(6, fields[1]); // item$oid
        assertEquals(7, fields[2]); // item$iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(5, indexKeyFields[0].fieldIndex()); // item$cid
        assertEquals(6, indexKeyFields[1].fieldIndex()); // item$oid
        assertEquals(7, indexKeyFields[2].fieldIndex()); // item$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(2, hKeyFields[5].indexKeyLoc()); // index iid
        // orders FK index
        index = index(coi, "orders$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(2, fields[0]); // orders$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(2, indexKeyFields[0].fieldIndex()); // orders$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        // item FK index
        index = index(coi, "item$cid", "item$oid", "item$iid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(5, fields[0]); // item$cid
        assertEquals(6, fields[1]); // item$oid
        assertEquals(7, fields[2]); // item$iid
        indexKeyFields = index.indexKeyFields();
        assertEquals(5, indexKeyFields[0].fieldIndex()); // item$cid
        assertEquals(6, indexKeyFields[1].fieldIndex()); // item$oid
        assertEquals(7, indexKeyFields[2].fieldIndex()); // item$oid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(1, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(2, hKeyFields[5].indexKeyLoc()); // index iid
        // customer cx
        index = index(coi, "customer$cx");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(1, fields[0]); // customer$cx
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // customer$cx
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        // orders ox, cid
        index = index(coi, "orders$ox", "orders$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(4, fields[0]); // orders$ox
        assertEquals(2, fields[1]); // orders$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(4, indexKeyFields[0].fieldIndex()); // orders$ox
        assertEquals(2, indexKeyFields[1].fieldIndex()); // orders$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(1, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        // item ix, iid, oid, cid
        index = index(coi, "item$ix", "item$iid", "item$oid", "item$cid");
        assertNotNull(index);
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        fields = index.getFields();
        assertEquals(8, fields[0]); // item$ix
        assertEquals(7, fields[1]); // item$iid
        assertEquals(6, fields[2]); // item$oid
        assertEquals(5, fields[3]); // item$cid
        indexKeyFields = index.indexKeyFields();
        assertEquals(8, indexKeyFields[0].fieldIndex()); // item$ix
        assertEquals(7, indexKeyFields[1].fieldIndex()); // item$iid
        assertEquals(6, indexKeyFields[2].fieldIndex()); // item$oid
        assertEquals(5, indexKeyFields[3].fieldIndex()); // item$cid
        hKeyFields = index.hkeyFields();
        assertEquals(customer.getOrdinal(), hKeyFields[0].ordinal()); // c ordinal
        assertEquals(3, hKeyFields[1].indexKeyLoc()); // index cid
        assertEquals(orders.getOrdinal(), hKeyFields[2].ordinal()); // o ordinal
        assertEquals(2, hKeyFields[3].indexKeyLoc()); // index oid
        assertEquals(item.getOrdinal(), hKeyFields[4].ordinal()); // i ordinal
        assertEquals(1, hKeyFields[5].indexKeyLoc()); // index iid
    }

    // PersistitStoreIndexManager.analyzeIndex relies on IndexDef.I2H.fieldIndex, but only for hkey equivalent
    // indexes. Given the original computation of index hkey equivalence, that means root tables and group table
    // indexes on root PK fields.
    @Test
    public void checkI2HFieldIndex() throws Exception
    {
        String[] ddl = {
            String.format("use `%s`;", SCHEMA),
            "create table parent (",
            "   a int,",
            "   b int,",
            "   x int,",
            "   primary key(b, a)",
            ") engine = akibandb;",
            "create table child (",
            "   c int,",
            "   d int,",
            "   b int,",
            "   a int,",
            "   x int,",
            "   primary key(c, d),",
            "   constraint `__akiban_fk0` foreign key `akibafk` (b, a) references parent(b, a)",
            ") engine = akibandb;"
        };
        RowDefCache rowDefCache = ROW_DEF_CACHE_FACTORY.rowDefCache(ddl);
        RowDef parent = rowDefCache.getRowDef(tableName("parent"));
        RowDef child = rowDefCache.getRowDef(tableName("child"));
        RowDef group = rowDefCache.getRowDef(parent.getGroupRowDefId());
        assertSame(group, rowDefCache.getRowDef(child.getGroupRowDefId()));
        assertTrue(index(parent, "b", "a").isHKeyEquivalent());
        assertTrue(index(child, "b", "a").isHKeyEquivalent()); 
        assertTrue(index(group, "parent$b", "parent$a").isHKeyEquivalent());
        assertTrue(!index(group, "child$c", "child$d").isHKeyEquivalent());
        assertTrue(index(group, "child$b", "child$a").isHKeyEquivalent()); 
        IndexDef index;
        // parent (b, a) index
        index = index(parent, "b", "a");
        assertEquals(parent.getOrdinal(), index.hkeyFields()[0].ordinal());
        assertEquals(1, index.hkeyFields()[1].fieldIndex());
        assertEquals(0, index.hkeyFields()[2].fieldIndex());
        // group (parent$b, parent$a) index
        index = index(group, "parent$b", "parent$a");
        assertEquals(parent.getOrdinal(), index.hkeyFields()[0].ordinal());
        assertEquals(1, index.hkeyFields()[1].fieldIndex());
        assertEquals(0, index.hkeyFields()[2].fieldIndex());
        // The remaining tests are for indexes that are hkey-equivalent only under the new computation of IndexDef
        // field associations.
        // child (b, a) index
        index = index(child, "b", "a");
        assertEquals(parent.getOrdinal(), index.hkeyFields()[0].ordinal());
        assertEquals(2, index.hkeyFields()[1].fieldIndex());
        assertEquals(3, index.hkeyFields()[2].fieldIndex());
        // group (child$b, child$a) index
        index = index(group, "child$b", "child$a");
        assertEquals(parent.getOrdinal(), index.hkeyFields()[0].ordinal());
        assertEquals(5, index.hkeyFields()[1].fieldIndex());
        assertEquals(6, index.hkeyFields()[2].fieldIndex());
    }

    private void checkFields(RowDef rowdef, String... expectedFields)
    {
        FieldDef[] fields = rowdef.getFieldDefs();
        Assert.assertEquals(expectedFields.length, fields.length);
        for (int i = 0; i < fields.length; i++) {
            assertEquals(expectedFields[i], fields[i].getName());
        }
    }

    private IndexDef index(RowDef rowDef, String... indexColumnNames)
    {
        for (IndexDef indexDef : rowDef.getIndexDefs()) {
            int[] indexFields = indexDef.getFields();
            boolean match = indexFields.length == indexColumnNames.length;
            for (int i = 0; match && i < indexColumnNames.length; i++) {
                if (!indexDef.getRowDef().getFieldDefs()[indexFields[i]].getName().equals(indexColumnNames[i])) {
                    match = false;
                }
            }
            if (match) {
                return indexDef;
            }
        }
        return null;
    }

    private String tableName(String name)
    {
        return String.format("%s.%s", SCHEMA, name);
    }

    private void checkField(String name, RowDef rowDef, int fieldNumber)
    {
        FieldDef field = rowDef.getFieldDefs()[fieldNumber];
        assertEquals(name, field.getName());
    }

    // Copied from AISTest
    private void checkHKey(HKey hKey, Object... elements)
    {
        int e = 0;
        int position = 0;
        for (HKeySegment segment : hKey.segments()) {
            Assert.assertEquals(position++, segment.positionInHKey());
            assertSame(elements[e++], segment.table());
            for (HKeyColumn column : segment.columns()) {
                Assert.assertEquals(position++, column.positionInHKey());
                Assert.assertEquals(elements[e++], column.column().getTable());
                Assert.assertEquals(elements[e++], column.column().getName());
            }
        }
        Assert.assertEquals(elements.length, e);
    }

    private static final String SCHEMA = "schema";
    private static final RowDefCacheFactory ROW_DEF_CACHE_FACTORY = new RowDefCacheFactory();
}
