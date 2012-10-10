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

package com.akiban.ais.model;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.util.Iterator;
import java.util.List;

import com.akiban.server.rowdata.SchemaFactory;
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
        UserTable table = ais.getUserTable("s", "t");
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
        UserTable table = ais.getUserTable("s", "t");
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
        UserTable table = ais.getUserTable("s", "t");
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
        Join join = ais.getUserTable("s", "child").getParentJoin();
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
        UserTable customer = ais.getUserTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("s", "item");
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
        UserTable customer = ais.getUserTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("s", "item");
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
        UserTable customer = ais.getUserTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("s", "item");
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
        UserTable customer = ais.getUserTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid0", customer, "cid1");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid0", order, "cid1",
                  order, order, "oid0", order, "oid1");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("s", "item");
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
        UserTable customer = ais.getUserTable("s", "customer");
        checkHKey(customer.hKey(),
                  customer, customer, "cid");
        // ---------------- Order -------------------------------------
        UserTable order = ais.getUserTable("s", "order");
        checkHKey(order.hKey(),
                  customer, order, "cid",
                  order, order, "oid");
        // ---------------- Item -------------------------------------
        UserTable item = ais.getUserTable("s", "item");
        checkHKey(item.hKey(),
                  customer, order, "cid",
                  order, item, "oid",
                  item, item, "iid");
        // ---------------- Address -------------------------------------
        UserTable address = ais.getUserTable("s", "address");
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
        UserTable table = (UserTable) ais.getTable("s", "t");
        // check columns
        checkColumns(table.getColumns(), "a", "b");
        checkColumns(table.getColumnsIncludingInternal(), "a", "b", Column.AKIBAN_PK_NAME);
        // check indexes
        assertTrue(table.getIndexes().isEmpty());
        assertEquals(1, table.getIndexesIncludingInternal().size());
        Index index = table.getIndexesIncludingInternal().iterator().next();
        assertEquals(Column.AKIBAN_PK_NAME, index.getKeyColumns().get(0).getColumn().getName());
        // check PK
        assertNull(table.getPrimaryKey());
        assertSame(table.getIndexesIncludingInternal().iterator().next(), table.getPrimaryKeyIncludingInternal().getIndex());
    }

    @Test
    public void testTypeSupport() throws Exception
    {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        // Basic type, case insensitivity and exists
        assertNotNull(ais.getType("int"));
        assertNotNull(ais.getType("inT"));
        assertNotNull(ais.getType("INT"));
        assertTrue(ais.isTypeSupported("int"));
        // Type exists but isn't supported
        assertNotNull(ais.getType("geometry"));
        assertFalse(ais.isTypeSupported("geometry"));
        // Unknown type returns null and false
        assertNull(ais.getType("not_a_real_type"));
        assertFalse(ais.isTypeSupported("not_a_real_type"));
    }

    @Test
    public void testTypesCanBeJoined() throws Exception {
        AkibanInformationSchema ais = new AkibanInformationSchema();
        // Every time can be joined to itself
        for(Type t : ais.getTypes()) {
            ais.canTypesBeJoined(t.name(), t.name());
        }
        // All int types can be joined together
        final String intTypeNames[] = {"tinyint", "smallint", "int", "mediumint", "bigint"};
        for(String t1 : intTypeNames) {
            String t1U = t1 + " unsigned";
            for(String t2 : intTypeNames) {
                String t2U = t2 + " unsigned";
                assertTrue(t1+"->"+t2, ais.canTypesBeJoined(t1, t2));
                assertTrue(t1U+"->"+t2, ais.canTypesBeJoined(t1U, t2));
                assertTrue(t1+"->"+t2U, ais.canTypesBeJoined(t1, t2U));
                assertTrue(t1U+"->"+t2U, ais.canTypesBeJoined(t1U, t2U));
            }
        }
        // Check a few that cannot be
        assertFalse(ais.canTypesBeJoined("int", "varchar"));
        assertFalse(ais.canTypesBeJoined("int", "timestamp"));
        assertFalse(ais.canTypesBeJoined("int", "decimal"));
        assertFalse(ais.canTypesBeJoined("int", "double"));
        assertFalse(ais.canTypesBeJoined("char", "binary"));
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
