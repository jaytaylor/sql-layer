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

package com.akiban.server;

import com.akiban.ais.model.*;
import junit.framework.Assert;
import org.junit.Test;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PKLessTableRowDefCacheTest
{
    @Test
    public void testPKLessRoot() throws Exception
    {
        String[] ddl = {
            String.format("use %s;", SCHEMA),
            "create table test(",
            "    a int, ",
            "    b int, ",
            "    c int, ",
            "    d int, ",
            "    e int, ",
            "    key e_d(e, d), ",
            "    unique key d_b(d, b)",
            ");"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        RowDef test = rowDefCache.getRowDef(tableName("test"));
        UserTable t = (UserTable) test.table();
        assertEquals(2, test.getHKeyDepth()); // test ordinal, test row counter
        checkHKey(t.hKey(), t, t, Column.AKIBAN_PK_NAME);
        Index index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // e, d index
        index = t.getIndex("e_d");
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        rowComp = index.indexRowComposition();
        assertEquals(4, rowComp.getFieldPosition(0)); // test.e
        assertEquals(3, rowComp.getFieldPosition(1)); // test.d
        assertEquals(5, rowComp.getFieldPosition(2)); // Akiban PK
        indexToHKey = index.indexToHKey();
        assertEquals(test.getOrdinal(), indexToHKey.getOrdinal(0)); // test ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1)); // test row counter
        // d, b index
        index = t.getIndex("d_b");
        assertTrue(!index.isPrimaryKey());
        assertTrue(index.isUnique());
        rowComp = index.indexRowComposition();
        assertEquals(3, rowComp.getFieldPosition(0)); // test.d
        assertEquals(1, rowComp.getFieldPosition(1)); // test.b
        assertEquals(5, rowComp.getFieldPosition(2)); // Akiban PK
        indexToHKey = index.indexToHKey();
        assertEquals(test.getOrdinal(), indexToHKey.getOrdinal(0)); // test ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1)); // Akiban PK
    }

    @Test
    public void testPKLessNonRoot() throws Exception
    {
        String[] ddl = {
            String.format("use %s;", SCHEMA),
            "create table parent(",
            "    p1 int, ",
            "    p2 int, ",
            "    primary key(p1)",
            "); ",
            "create table child(",
            "    c1 int, ",
            "    c2 int, ",
            "    p1 int, ",
            "    constraint __akiban_fk foreign key fk(p1) references parent(p1), ",
            "    key c2_c1(c2, c1)",
            ");"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        Index index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // ------------------------- parent ----------------------------------------------------------------------------
        RowDef parent = rowDefCache.getRowDef(tableName("parent"));
        UserTable p = (UserTable) parent.table();
        assertEquals(2, parent.getHKeyDepth()); // parent ordinal, p1
        checkHKey(p.hKey(), p, p, "p1");
        // PK index
        index = p.getPrimaryKey().getIndex();
        assertTrue(index.isPrimaryKey());
        assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        rowComp = index.indexRowComposition();
        assertEquals(0, rowComp.getFieldPosition(0)); // parent.p1
        indexToHKey = index.indexToHKey();
        assertEquals(parent.getOrdinal(), indexToHKey.getOrdinal(0)); // parent ordinal
        assertEquals(0, indexToHKey.getIndexRowPosition(1)); // parent p1
        // ------------------------- child -----------------------------------------------------------------------------
        RowDef child = rowDefCache.getRowDef(tableName("child"));
        UserTable c = (UserTable) child.table();
        assertEquals(2, parent.getHKeyDepth()); // child ordinal, child row counter
        checkHKey(c.hKey(),
                  p, c, "p1",
                  c, c, Column.AKIBAN_PK_NAME);
        // c2, c1 index
        index = c.getIndex("c2_c1");
        assertTrue(!index.isPrimaryKey());
        assertTrue(!index.isUnique());
        // assertTrue(!index.isHKeyEquivalent());
        rowComp = index.indexRowComposition();
        assertEquals(1, rowComp.getFieldPosition(0)); // child.c2
        assertEquals(0, rowComp.getFieldPosition(1)); // child.c1
        indexToHKey = index.indexToHKey();
        assertEquals(parent.getOrdinal(), indexToHKey.getOrdinal(0)); // parent ordinal
        assertEquals(2, indexToHKey.getFieldPosition(1)); // child p1
        assertEquals(child.getOrdinal(), indexToHKey.getOrdinal(2)); // child ordinal
        assertEquals(2, indexToHKey.getIndexRowPosition(1)); // child row counter
    }

    private String tableName(String name)
    {
        return RowDefCache.nameOf(SCHEMA, name);
    }

    // Copied from AISTest, generalized for pk less tables
    private void checkHKey(HKey hKey, Object... elements)
    {
        int e = 0;
        int position = 0;
        for (HKeySegment segment : hKey.segments()) {
            Assert.assertEquals(position++, segment.positionInHKey());
            assertSame(elements[e++], segment.table());
            for (HKeyColumn hKeyColumn : segment.columns()) {
                Assert.assertEquals(position++, hKeyColumn.positionInHKey());
                Object expectedTable = elements[e++];
                String expectedColumnName = (String) elements[e++];
                Column column = hKeyColumn.column();
                Assert.assertEquals(expectedTable, column.getTable());
                Assert.assertEquals(expectedColumnName, hKeyColumn.column().getName());
            }
        }
        Assert.assertEquals(elements.length, e);
    }

    private static final String SCHEMA = "schema";
    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory();
}
