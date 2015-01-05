/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;

import com.foundationdb.server.rowdata.SchemaFactory;
import org.junit.Test;

public class AISTest
{
    @Test
    public void testTableColumnsReturnedInOrder() throws Exception
    {
        String[] ddl = {
            "create table s.t(",
            "    col2 int not null, ",
            "    col1 int not null, ",
            "    col0 int not null ",
            ");"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Table table = ais.getTable("s", "t");
        int expectedPosition = 0;
        for (Column column : table.getColumns()) {
            assertEquals(expectedPosition, column.getPosition().intValue());
            expectedPosition++;
        }
    }

    @Test
    public void testIndexColumnsReturnedInOrder() throws Exception
    {
        String[] ddl = {
            "create table s.t(",
            "    col0 int not null, ",
            "    col1 int not null, ",
            "    col2 int not null, ",
            "    col3 int not null, ",
            "    col4 int not null, ",
            "    col5 int not null ",
            ");",
            "create index i on s.t(col5, col4, col3);"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Table table = ais.getTable("s", "t");
        Index index = table.getIndex("i");
        Iterator<IndexColumn> indexColumnScan = index.getKeyColumns().iterator();
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
    public void testPKColumnsReturnedInOrder() throws Exception
    {
        String[] ddl = {
            "create table s.t(",
            "    col0 int not null, ",
            "    col1 int not null, ",
            "    col2 int not null, ",
            "    col3 int not null, ",
            "    col4 int not null, ",
            "    col5 int not null, ",
            "    primary key (col5, col4, col3) ",
            ");"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Table table = ais.getTable("s", "t");
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
    public void testJoinColumnsReturnedInOrder() throws Exception
    {
        String[] ddl = {
            "create table s.parent(",
            "    p0 int not null, ",
            "    p1 int not null, ",
            "    primary key (p1, p0) ",
            ");",
            "create table s.child(",
            "    c0 int not null, ",
            "    c1 int not null, ",
            "    primary key (c0, c1), ",
            "    grouping foreign key (c0, c1) references parent(p1, p0)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Join join = ais.getTable("s", "child").getParentJoin();
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
    public void testHKeyNonCascadingPKs() throws Exception
    {
        String[] ddl = {
            "create table s.customer(",
            "    cid int not null, ",
            "    primary key (cid) ",
            ");",
            "create table s.\"order\"(",
            "    oid int not null, ",
            "    cid int not null, ",
            "    primary key (oid), ",
            "    grouping foreign key (cid) references customer(cid)",
            ");",
            "create table s.item(",
            "    iid int not null, ",
            "    oid int not null, ",
            "    primary key (iid), ",
            "    grouping foreign key (oid) references \"order\"(oid)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        // ---------------- Customer -------------------------------------
        Table customer = ais.getTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        Table order = ais.getTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        Table item = ais.getTable("s", "item");
        checkHKey(item.hKey(),
                  customer, order, "cid",
                  order, item, "oid",
                  item, item, "iid");
    }

    @Test
    public void testHKeyNonCascadingMultiColumnPKs() throws Exception
    {
        String[] ddl = {
            "create table s.customer(",
            "    cid0 int not null, ",
            "    cid1 int not null, ",
            "    primary key (cid0, cid1) ",
            ");",
            "create table s.\"order\"(",
            "    oid0 int not null, ",
            "    oid1 int not null, ",
            "    cid0 int not null, ",
            "    cid1 int not null, ",
            "    primary key (oid0, oid1), ",
            "    grouping foreign key (cid0, cid1) references customer(cid0, cid1)",
            ");",
            "create table s.item(",
            "    iid0 int not null, ",
            "    iid1 int not null, ",
            "    oid0 int not null, ",
            "    oid1 int not null, ",
            "    primary key (iid0, iid1), ",
            "    grouping foreign key (oid0, oid1) references \"order\"(oid0, oid1)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        // ---------------- Customer -------------------------------------
        Table customer = ais.getTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        Table order = ais.getTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        Table item = ais.getTable("s", "item");
        checkHKey(item.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, item, "oid0", item, "oid1",
                  item, item, "iid0", item, "iid1");
    }

    @Test
    public void testHKeyCascadingPKs() throws Exception
    {
        String[] ddl = {
            "create table s.customer(",
            "    cid int not null, ",
            "    primary key (cid) ",
            ");",
            "create table s.\"order\"(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    primary key (cid, oid), ",
            "    grouping foreign key (cid) references customer(cid)",
            ");",
            "create table s.item(",
            "    cid int not null, ",
            "    oid int not null, ",
            "    iid int not null, ",
            "    primary key (cid, oid, iid), ",
            "    grouping foreign key (cid, oid) references \"order\"(cid, oid)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        // ---------------- Customer -------------------------------------
        Table customer = ais.getTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        Table order = ais.getTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        Table item = ais.getTable("s", "item");
        checkHKey(item.hKey(),
                  customer, item, "cid",
                  order, item, "oid",
                  item, item, "iid");
    }

    @Test
    public void testHKeyCascadingMultiColumnPKs() throws Exception
    {
        String[] ddl = {
            "create table s.customer(",
            "    cid0 int not null, ",
            "    cid1 int not null, ",
            "    primary key (cid0, cid1) ",
            ");",
            "create table s.\"order\"(",
            "    cid0 int not null, ",
            "    cid1 int not null, ",
            "    oid0 int not null, ",
            "    oid1 int not null, ",
            "    primary key (cid0, cid1, oid0, oid1), ",
            "    grouping foreign key (cid0, cid1) references customer(cid0, cid1)",
            ");",
            "create table s.item(",
            "    cid0 int not null, ",
            "    cid1 int not null, ",
            "    oid0 int not null, ",
            "    oid1 int not null, ",
            "    iid0 int not null, ",
            "    iid1 int not null, ",
            "    primary key (cid0, cid1, oid0, oid1, iid0, iid1), ",
            "    grouping foreign key (cid0, cid1, oid0, oid1) references \"order\"(cid0, cid1, oid0, oid1)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        // ---------------- Customer -------------------------------------
        Table customer = ais.getTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        Table order = ais.getTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        Table item = ais.getTable("s", "item");
        checkHKey(item.hKey(),
                  customer, item, "cid0", item, "cid1",
                  order, item, "oid0", item, "oid1",
                  item, item, "iid0", item, "iid1");
    }

    @Test
    public void testHKeyWithBranches() throws Exception
    {
        String[] ddl = {
            "create table s.customer(",
            "    cid int not null, ",
            "    primary key (cid) ",
            ");",
            "create table s.\"order\"(",
            "    oid int not null, ",
            "    cid int not null, ",
            "    primary key (oid), ",
            "    grouping foreign key (cid) references customer(cid)",
            ");",
            "create table s.item(",
            "    iid int not null, ",
            "    oid int not null, ",
            "    primary key (iid), ",
            "    grouping foreign key (oid) references \"order\"(oid)",
            ");",
            "create table s.address(",
            "    aid int not null, ",
            "    cid int not null, ",
            "    primary key (aid), ",
            "    grouping foreign key (cid) references customer(cid)",
            ");",
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        // ---------------- Customer -------------------------------------
        Table customer = ais.getTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        Table order = ais.getTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        Table item = ais.getTable("s", "item");
        checkHKey(item.hKey(),
                  customer, order, "cid",
                  order, item, "oid",
                  item, item, "iid");
        // ---------------- Address -------------------------------------
        Table address = ais.getTable("s", "address");
        checkHKey(address.hKey(),
                  customer, address, "cid",
                  address, address, "aid");
    }

    @Test
    public void testAkibanPKColumn() throws Exception
    {
        String[] ddl = {
            "create table s.t(",
            "    a int, ",
            "    b int",
            ");"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.ais(ddl);
        Table table = ais.getTable("s", "t");
        // check columns
        checkColumns(table.getColumns(), "a", "b");
        checkColumns(table.getColumnsIncludingInternal(), "a", "b", Column.ROW_ID_NAME);
        // check indexes
        assertTrue(table.getIndexes().isEmpty());
        assertEquals(1, table.getIndexesIncludingInternal().size());
        Index index = table.getIndexesIncludingInternal().iterator().next();
        assertEquals(Column.ROW_ID_NAME, index.getKeyColumns().get(0).getColumn().getName());
        // check PK
        assertNull(table.getPrimaryKey());
        assertSame(table.getIndexesIncludingInternal().iterator().next(), table.getPrimaryKeyIncludingInternal().getIndex());
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

    private void checkColumns(List<Column> actual, String ... expected)
    {
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual.get(i).getName());
        }
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory("s");
}
