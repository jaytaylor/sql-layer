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

package com.foundationdb.server.rowdata;

import com.foundationdb.ais.model.*;
import junit.framework.Assert;
import org.junit.Test;

import static junit.framework.Assert.assertSame;

public class PKLessTableRowDefBuilderTest
{
    @Test
    public void testPKLessRoot() throws Exception
    {
        String[] ddl = {
            "create table test(",
            "    a int, ",
            "    b int, ",
            "    c int, ",
            "    d int, ",
            "    e int, ",
            "    constraint d_b unique(d, b)",
            ");",
            "create index e_d on test(e, d);"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.aisWithRowDefs(ddl);
        RowDef test = ais.getTable(tableName("test")).rowDef();
        Table t = test.table();
        Assert.assertEquals(2, test.getHKeyDepth()); // test ordinal, test row counter
        checkHKey(t.hKey(), t, t, Column.ROW_ID_NAME);
        TableIndex index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // e, d index
        index = t.getIndex("e_d");
        Assert.assertTrue(!index.isPrimaryKey());
        Assert.assertTrue(!index.isUnique());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(4, rowComp.getFieldPosition(0)); // test.e
        Assert.assertEquals(3, rowComp.getFieldPosition(1)); // test.d
        Assert.assertEquals(5, rowComp.getFieldPosition(2)); // hidden PK
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(t.getOrdinal().intValue(), indexToHKey.getOrdinal(0)); // test ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // test row counter
        // d, b index
        index = t.getIndex("d_b");
        Assert.assertTrue(!index.isPrimaryKey());
        Assert.assertTrue(index.isUnique());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(3, rowComp.getFieldPosition(0)); // test.d
        Assert.assertEquals(1, rowComp.getFieldPosition(1)); // test.b
        Assert.assertEquals(5, rowComp.getFieldPosition(2)); // hidden PK
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(t.getOrdinal().intValue(), indexToHKey.getOrdinal(0)); // test ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // hidden PK
    }

    @Test
    public void testPKLessNonRoot() throws Exception
    {
        String[] ddl = {
            "create table parent(",
            "    p1 int not null, ",
            "    p2 int, ",
            "    primary key(p1)",
            "); ",
            "create table child(",
            "    c1 int, ",
            "    c2 int, ",
            "    p1 int, ",
            "    grouping foreign key(p1) references parent(p1)",
            ");",
            "create index c2_c1 on child(c2, c1);"
        };
        AkibanInformationSchema ais = SCHEMA_FACTORY.aisWithRowDefs(ddl);
        TableIndex index;
        IndexRowComposition rowComp;
        IndexToHKey indexToHKey;
        // ------------------------- parent ----------------------------------------------------------------------------
        RowDef parent = ais.getTable(tableName("parent")).rowDef();
        Table p = parent.table();
        Assert.assertEquals(2, parent.getHKeyDepth()); // parent ordinal, p1
        checkHKey(p.hKey(), p, p, "p1");
        // PK index
        index = p.getPrimaryKey().getIndex();
        Assert.assertTrue(index.isPrimaryKey());
        Assert.assertTrue(index.isUnique());
        // assertTrue(index.isHKeyEquivalent());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(0, rowComp.getFieldPosition(0)); // parent.p1
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(p.getOrdinal().intValue(), indexToHKey.getOrdinal(0)); // parent ordinal
        Assert.assertEquals(0, indexToHKey.getIndexRowPosition(1)); // parent p1
        // ------------------------- child -----------------------------------------------------------------------------
        RowDef child = ais.getTable(tableName("child")).rowDef();
        Table c = child.table();
        Assert.assertEquals(2, parent.getHKeyDepth()); // child ordinal, child row counter
        checkHKey(c.hKey(),
                  p, c, "p1",
                  c, c, Column.ROW_ID_NAME);
        // c2, c1 index. Row is (c.c2, c.c1, c.p1, c.HIDDEN_PK)
        index = c.getIndex("c2_c1");
        Assert.assertTrue(!index.isPrimaryKey());
        Assert.assertTrue(!index.isUnique());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(1, rowComp.getFieldPosition(0)); // child.c2
        Assert.assertEquals(0, rowComp.getFieldPosition(1)); // child.c1
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(p.getOrdinal().intValue(), indexToHKey.getOrdinal(0)); // parent ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // child p1
        Assert.assertEquals(c.getOrdinal().intValue(), indexToHKey.getOrdinal(2)); // child ordinal
        Assert.assertEquals(3, indexToHKey.getIndexRowPosition(3)); // child row counter
    }

    private TableName tableName(String name)
    {
        return new TableName(SCHEMA, name);
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
    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory(SCHEMA);
}
