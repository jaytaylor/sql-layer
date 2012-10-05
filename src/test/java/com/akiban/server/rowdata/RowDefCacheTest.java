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

package com.akiban.server.rowdata;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.*;

import com.akiban.ais.model.*;
import junit.framework.Assert;

import org.junit.Test;

public class RowDefCacheTest
{
    @Test
    public void testMultipleBadlyOrderedColumns() throws Exception
    {
        String[] ddl = {
            "create table b(",
            "    b0 int,",
            "    b1 int not null,",
            "    b2 int not null,",
            "    b3 int not null,",
            "    b4 int not null,",
            "    b5 int,",
            "    primary key(b3, b2, b4, b1)",
            ");",
            "create table bb(",
            "    bb0 int not null,",
            "    bb1 int,",
            "    bb2 int not null,",
            "    bb3 int not null,",
            "    bb4 int not null,",
            "    bb5 int not null,",
            "    primary key (bb0, bb5, bb3, bb2, bb4), ",
            "    GROUPING FOREIGN KEY (bb0,bb2,bb1,bb3) REFERENCES b (b3,b2,b4,b1)",
            ");",
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
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
        assertArrayEquals(new int[]{}, b.getParentJoinFields());
        assertArrayEquals(new int[]{0, 2, 1, 3}, bb.getParentJoinFields());
        assertEquals(b.getRowDefId(), bb.getParentRowDefId());
        assertEquals(0, b.getParentRowDefId());
    }

    @Test
    public void childDoesNotContributeToHKey() throws Exception
    {
        String[] ddl = {
            "create table parent (",
            "   id int not null,",
            "   primary key(id)",
            ");",
            "create table child (",
            "   id int not null,",
            "   primary key(id),",
            "   GROUPING FOREIGN KEY (id) references parent(id)",
            ");"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
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
        assertArrayEquals("parent joins", new int[]{}, parent.getParentJoinFields());
        assertArrayEquals("child joins", new int[]{0}, child.getParentJoinFields());
    }

    @Test
    public void testUserIndexDefs() throws Exception
    {
        String[] ddl = {
            "create table t (",
            "    a int not null, ",
            "    b int, ",
            "    c int not null, ",
            "    d int, ",
            "    e int, ",
            "    primary key(c, a), ",
            "    constraint d_b unique(d, b)",
            ");",
            "create index e_d on t(e, d);"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        RowDef t = rowDefCache.getRowDef(tableName("t"));
        assertEquals(3, t.getHKeyDepth()); // t ordinal, c, a
        Index index;
        index = t.getPKIndex();
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        index = t.getIndex("e_d");
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        index = t.getIndex("d_b");
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
    }

    @Test
    public void testNonCascadingPKs() throws Exception
    {
        String[] ddl = {
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid), ",
            "    constraint cid_cx unique(cid, cx) ",
            "); ",
            "create table orders(",
            "    oid int not null, ",
            "    cid int not null, ",
            "    ox int not null, ",
            "    primary key(oid), ",
            "    constraint cid_oid unique(cid, oid), ",
            "    constraint oid_cid unique(oid, cid), ",
            "    constraint cid_oid_ox unique(cid, oid, ox), ",
            "    grouping foreign key(cid) references customer(cid)",
            "); ",
            "create index \"__akiban_oc\" on orders(cid);",
            "create table item(",
            "    iid int not null, ",
            "    oid int not null, ",
            "    ix int not null, ",
            "    primary key(iid), ",
            "    grouping foreign key(oid) references orders(oid)",
            "); ",
            "create index \"__akiban_io\" on item(oid);",
            "create index oid_iid on item(oid, iid);",
            "create index iid_oid on item(iid, oid);",
            "create index oid_iid_ix on item(oid, iid, ix);",
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        TableIndex index;
        int[] fields;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // ------------------------- Customer ------------------------------------------
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "cid", "cx");
        assertEquals(2, customer.getHKeyDepth()); // customer ordinal, cid
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // index on cid
        index = customer.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // c.cid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // c.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        // index on cid, cx
        index = customer.getIndex("cid_cx");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // c.cid
        assertEquals(1, fields[1]); // c.cx
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // c.cid
        assertEquals(1, rowComp.getFieldPosition(1)); // c.cx
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        // ------------------------- Orders ------------------------------------------
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "oid", "cid", "ox");
        assertEquals(4, orders.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid
        assertArrayEquals(new int[]{1}, orders.getParentJoinFields());
        // index on oid
        index = orders.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // o.oid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // o.oid
        assertEquals(1, rowComp.getFieldPosition(1)); // o.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(3)); // index oid
        // index on cid, oid
        index = orders.getIndex("cid_oid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(1, fields[0]); // o.cid
        assertEquals(0, fields[1]); // o.oid
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // o.cid
        assertEquals(0, rowComp.getFieldPosition(1)); // o.oid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1));
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(3)); // index oid
        // index on oid, cid
        index = orders.getIndex("oid_cid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // o.oid
        assertEquals(1, fields[1]); // o.cid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // o.oid
        assertEquals(1, rowComp.getFieldPosition(1)); // o.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(3)); // index oid
        // index on cid, oid, ox
        index = orders.getIndex("cid_oid_ox");
        assertNotNull(index);
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(1, fields[0]); // o.cid
        assertEquals(0, fields[1]); // o.oid
        assertEquals(2, fields[2]); // o.ox
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // o.cid
        assertEquals(0, rowComp.getFieldPosition(1)); // o.oid
        assertEquals(2, rowComp.getFieldPosition(2)); // o.ox
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(3)); // index oid
        // ------------------------- Item ------------------------------------------
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "iid", "oid", "ix");
        assertEquals(6, item.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid, item ordinal, iid
        assertArrayEquals(new int[]{1}, item.getParentJoinFields());
        // Index on iid
        index = item.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // i.iid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // i.iid
        assertEquals(1, rowComp.getHKeyPosition(1)); // hkey cid
        assertEquals(1, rowComp.getFieldPosition(2)); // i.oid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(5)); // index oid
        // Index on oid, iid
        index = item.getIndex("oid_iid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(1, fields[0]); // i.oid
        assertEquals(0, fields[1]); // i.iid
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // i.oid
        assertEquals(0, rowComp.getFieldPosition(1)); // i.iid
        assertEquals(1, rowComp.getHKeyPosition(2)); // hkey cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(5)); // index iid
        // Index on iid, oid
        index = item.getIndex("iid_oid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // i.iid
        assertEquals(1, fields[1]); // i.oid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // i.iid
        assertEquals(1, rowComp.getFieldPosition(1)); // i.oid
        assertEquals(1, rowComp.getHKeyPosition(2)); // hkey cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(5)); // index iid
        // Index on oid, iid, ix
        index = item.getIndex("oid_iid_ix");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(1, fields[0]); // i.oid
        assertEquals(0, fields[1]); // i.iid
        assertEquals(2, fields[2]); // i.ix
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // i.oid
        assertEquals(0, rowComp.getFieldPosition(1)); // i.iid
        assertEquals(2, rowComp.getFieldPosition(2)); // i.ix
        assertEquals(1, rowComp.getHKeyPosition(3)); // hkey cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(5)); // index iid
    }

    @Test
    public void testCascadingPKs() throws Exception
    {
        String[] ddl = {
            "create table customer(",
            "    cid int not null, ",
            "    cx int not null, ",
            "    primary key(cid) ",
            "); ",
            "create index cx on customer(cx);",
            "create table orders(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    ox int not null, ",
            "    primary key(cid, oid), ",
            "    grouping foreign key (cid) references customer(cid)",
            "); ",
            "create index \"__akiban_oc\" on orders(cid);",
            "create index ox_cid on orders(ox, cid);",
            "create table item(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    iid int not null, ",
            "    ix int not null, ",
            "    primary key(cid, oid, iid), ",
            "    grouping foreign key (cid, oid) references orders(cid, oid)",
            "); ",
            "create index \"__akiban_io\" on item(cid, oid);",
            "create index ix_iid_oid_cid on item(ix, iid, oid, cid);",
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        TableIndex index;
        int[] fields;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // ------------------------- Customer ------------------------------------------
        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        checkFields(customer, "cid", "cx");
        assertEquals(2, customer.getHKeyDepth()); // customer ordinal, cid
        assertArrayEquals(new int[]{}, customer.getParentJoinFields());
        // index on cid
        index = customer.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // c.cid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // c.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        // index on cx
        index = customer.getIndex("cx");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(1, fields[0]); // c.cx
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // c.cx
        assertEquals(0, rowComp.getFieldPosition(1)); // c.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(1)); // index cid
        // ------------------------- Orders ------------------------------------------
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        checkFields(orders, "cid", "oid", "ox");
        assertEquals(4, orders.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid
        assertArrayEquals(new int[]{0}, orders.getParentJoinFields());
        // index on cid, oid
        index = orders.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // o.cid
        assertEquals(1, fields[1]); // o.oid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // o.cid
        assertEquals(1, rowComp.getFieldPosition(1)); // o.oid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(3)); // index oid
        // index on ox, cid
        index = orders.getIndex("ox_cid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(2, fields[0]); // o.ox
        assertEquals(0, fields[1]); // o.cid
        rowComp = index.indexRowComposition();
        assertEquals(2, rowComp.getFieldPosition(0)); // o.ox
        assertEquals(0, rowComp.getFieldPosition(1)); // o.cid
        assertEquals(1, rowComp.getFieldPosition(2)); // o.oid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(3)); // index oid
        // ------------------------- Item ------------------------------------------
        RowDef item = rowDefCache.getRowDef(tableName("item"));
        checkFields(item, "cid", "oid", "iid", "ix");
        assertEquals(6, item.getHKeyDepth()); // customer ordinal, cid, orders ordinal, oid, item ordinal iid
        assertArrayEquals(new int[]{0, 1}, item.getParentJoinFields());
        // index on cid, oid, iid
        index = item.getPKIndex();
        assertNotNull(index);
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(0, fields[0]); // i.cid
        assertEquals(1, fields[1]); // i.oid
        assertEquals(2, fields[2]); // i.iid
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // i.cid
        assertEquals(1, rowComp.getFieldPosition(1)); // i.oid
        assertEquals(2, rowComp.getFieldPosition(2)); // i.iid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(5)); // index oid
        // index on ix, iid, oid, cid
        index = item.getIndex("ix_iid_oid_cid");
        assertNotNull(index);
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        fields = indexFields(index);
        assertEquals(3, fields[0]); // i.ix
        assertEquals(2, fields[1]); // i.iid
        assertEquals(1, fields[2]); // i.oid
        assertEquals(0, fields[3]); // i.cid
        rowComp = index.indexRowComposition();
        assertEquals(3, rowComp.getFieldPosition(0)); // i.ix
        assertEquals(2, rowComp.getFieldPosition(1)); // i.iid
        assertEquals(1, rowComp.getFieldPosition(2)); // i.oid
        assertEquals(0, rowComp.getFieldPosition(3)); // i.cid
        indexToHKey = index.indexToHKey();
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(1)); // index cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2)); // o ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(3)); // index oid
        assertEquals(item.getOrdinal(), indexToHKey.getOrdinal(4)); // i ordinal
        assertEquals(1, indexToHKey.getIndexRowPosition(5)); // index oid
    }

    @Test
    public void checkCOGroupIndex() throws Exception {
        String[] ddl = { "create table customer(cid int not null, name varchar(32), primary key(cid));",
                         "create table orders(oid int not null, cid int, date date, primary key(oid), "+
                                 "grouping foreign key(cid) references customer(cid));",
                         "create index cName_oDate on orders(customer.name, orders.date) using left join;"
        };

        final RowDefCache rowDefCache;
        {
            AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
            Table customerTable = ais.getTable(SCHEMA, "customer");
            Table ordersTable = ais.getTable(SCHEMA, "orders");
            GroupIndex index = GroupIndex.create(ais,
                                                 customerTable.getGroup(),
                                                 "cName_oDate",
                                                 100,
                                                 false,
                                                 Index.KEY_CONSTRAINT,
                                                 Index.JoinType.LEFT);
            IndexColumn.create(index, customerTable.getColumn("name"), 0, true, null);
            IndexColumn.create(index, ordersTable.getColumn("date"), 1, true, null);
            rowDefCache = SCHEMA_FACTORY.rowDefCache(ais);
        }

        GroupIndex index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;

        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        // left join group index on (c.name,o.date):
        //     declared: c.name  o.date
        //     hkey: o.cid  c.oid
        index = orders.getGroupIndex("cName_oDate");
        assertNotNull(index);
        assertFalse(index.isPrimaryKey());
        assertFalse(index.isUnique());
        rowComp = index.indexRowComposition();
        // Flattened: (c.cid, c.name, o.oid, o.cid, o.date)
        assertEquals(1, rowComp.getFieldPosition(0)); // c.name
        assertEquals(4, rowComp.getFieldPosition(1)); // o.date
        // order hkey
        assertEquals(0, rowComp.getFieldPosition(2)); // c.cid
        assertEquals(2, rowComp.getFieldPosition(3)); // o.oid
        assertEquals(4, rowComp.getLength());
        // group index row: c.name, o.date, o.cid, o.oid, c.cid
        // group index -> order hkey
        indexToHKey = index.indexToHKey(orders.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1));            // c.cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2));   // o ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(3));            // o.oid
        // group index -> customer hkey
        indexToHKey = index.indexToHKey(customer.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1));            // c.cid
    }

    @Test
    public void checkCOIGroupIndex() throws Exception {
        String[] ddl = { "create table customer(cid int not null, name varchar(32), primary key(cid));",
                         "create table orders(oid int not null, cid int, date date, primary key(oid), "+
                                 "grouping foreign key(cid) references customer(cid));",
                         "create table items(iid int not null, oid int, sku int, primary key(iid), "+
                                 "grouping foreign key(oid) references orders(oid));",
                         "create index cName_oDate_iSku on items(customer.name, orders.date, items.sku) using left join;"
        };

        final RowDefCache rowDefCache;
        {
            AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
            rowDefCache = SCHEMA_FACTORY.rowDefCache(ais);
        }

        GroupIndex index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;

        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        RowDef items = rowDefCache.getRowDef(tableName("items"));
        // left join group index on (c.name,o.date,i.sku):
        //    declared: c.name  o.date  i.sku
        //    hkey:  c.cid  o.oid  i.iid
        index = items.getGroupIndex("cName_oDate_iSku");
        assertNotNull(index);
        assertFalse(index.isPrimaryKey());
        assertFalse(index.isUnique());
        rowComp = index.indexRowComposition();
        // Flattened: (c.cid, c.name, o.oid, o.cid, o.date, i.iid, i.oid, i.sku)
        assertEquals(1, rowComp.getFieldPosition(0)); // c.name
        assertEquals(4, rowComp.getFieldPosition(1)); // o.date
        assertEquals(7, rowComp.getFieldPosition(2)); // i.sku
        // item hkey
        assertEquals(0, rowComp.getFieldPosition(3)); // c.cid
        assertEquals(2, rowComp.getFieldPosition(4)); // o.oid
        assertEquals(5, rowComp.getFieldPosition(5)); // i.iid
        assertEquals(6, rowComp.getLength());
        // group index -> i hkey
        indexToHKey = index.indexToHKey(items.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(1));            // c.cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2));   // o ordinal
        assertEquals(4, indexToHKey.getIndexRowPosition(3));            // o.oid
        assertEquals(items.getOrdinal(), indexToHKey.getOrdinal(4));    // i ordinal
        assertEquals(5, indexToHKey.getIndexRowPosition(5));            // i.iid
        // group index -> o hkey
        indexToHKey = index.indexToHKey(orders.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(1));            // c.cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2));   // o ordinal
        assertEquals(4, indexToHKey.getIndexRowPosition(3));            // o.oid
        // group index -> c hkey
        indexToHKey = index.indexToHKey(customer.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(1));            // c.cid
    }

    @Test
    public void checkOIGroupIndex() throws Exception {
        String[] ddl = { "create table customer(cid int not null, name varchar(32), primary key(cid));",
                         "create table orders(oid int not null, cid int, date date, primary key(oid), "+
                                 "grouping foreign key(cid) references customer(cid));",
                         "create table items(iid int not null, oid int, sku int, primary key(iid), "+
                                 "grouping foreign key(oid) references orders(oid));",
                         "create index oDate_iSku on items(orders.date, items.sku) using left join;"
        };

        final RowDefCache rowDefCache;
        {
            AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
            rowDefCache = SCHEMA_FACTORY.rowDefCache(ais);
        }

        GroupIndex index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;

        RowDef customer = rowDefCache.getRowDef(tableName("customer"));
        RowDef orders = rowDefCache.getRowDef(tableName("orders"));
        RowDef items = rowDefCache.getRowDef(tableName("items"));
        // left join group index on o.date,i.sku
        //     declared:  o.date  i.sku
        //     hkey:  o.cid  o.oid  i.iid
        index = items.getGroupIndex("oDate_iSku");
        assertNotNull(index);
        assertFalse(index.isPrimaryKey());
        assertFalse(index.isUnique());
        rowComp = index.indexRowComposition();
        // Flattened: (c.cid, c.name, o.oid, o.cid, o.date, i.iid, i.oid, i.sku)
        assertEquals(4, rowComp.getFieldPosition(0)); // o.date
        assertEquals(7, rowComp.getFieldPosition(1)); // i.sku
        // item hkey
        assertEquals(3, rowComp.getFieldPosition(2)); // o.cid
        assertEquals(2, rowComp.getFieldPosition(3)); // o.oid
        assertEquals(5, rowComp.getFieldPosition(4)); // i.iid
        assertEquals(5, rowComp.getLength());
        // group index -> i hkey
        indexToHKey = index.indexToHKey(items.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1));            // o.cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2));   // o ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(3));            // o.oid
        assertEquals(items.getOrdinal(), indexToHKey.getOrdinal(4));    // i ordinal
        assertEquals(4, indexToHKey.getIndexRowPosition(5));            // i.iid
        // group index -> o hkey
        indexToHKey = index.indexToHKey(orders.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1));            // o.cid
        assertEquals(orders.getOrdinal(), indexToHKey.getOrdinal(2));   // o ordinal
        assertEquals(3, indexToHKey.getIndexRowPosition(3));            // o.oid
        // group index -> c hkey
        indexToHKey = index.indexToHKey(customer.userTable().getDepth());
        assertEquals(customer.getOrdinal(), indexToHKey.getOrdinal(0)); // c ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1));            // o.cid
    }

    private void checkFields(RowDef rowdef, String... expectedFields)
    {
        FieldDef[] fields = rowdef.getFieldDefs();
        Assert.assertEquals(expectedFields.length, fields.length);
        for (int i = 0; i < fields.length; i++) {
            assertEquals(expectedFields[i], fields[i].getName());
        }
    }

    private TableName tableName(String name)
    {
        return new TableName(SCHEMA, name);
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

    private int[] indexFields(Index index) {
        return index.indexDef().getFields();
    }

    private static final String SCHEMA = "schema";
    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory(SCHEMA);
}
