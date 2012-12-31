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

import com.akiban.ais.model.*;
import junit.framework.Assert;
import org.junit.Test;

import static junit.framework.Assert.assertSame;

public class PKLessTableRowDefCacheTest
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
        UserTable t = (UserTable) test.table();
        Assert.assertEquals(2, test.getHKeyDepth()); // test ordinal, test row counter
        checkHKey(t.hKey(), t, t, Column.AKIBAN_PK_NAME);
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
        Assert.assertEquals(5, rowComp.getFieldPosition(2)); // Akiban PK
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(test.getOrdinal(), indexToHKey.getOrdinal(0)); // test ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // test row counter
        // d, b index
        index = t.getIndex("d_b");
        Assert.assertTrue(!index.isPrimaryKey());
        Assert.assertTrue(index.isUnique());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(3, rowComp.getFieldPosition(0)); // test.d
        Assert.assertEquals(1, rowComp.getFieldPosition(1)); // test.b
        Assert.assertEquals(5, rowComp.getFieldPosition(2)); // Akiban PK
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(test.getOrdinal(), indexToHKey.getOrdinal(0)); // test ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // Akiban PK
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
        UserTable p = (UserTable) parent.table();
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
        Assert.assertEquals(parent.getOrdinal(), indexToHKey.getOrdinal(0)); // parent ordinal
        Assert.assertEquals(0, indexToHKey.getIndexRowPosition(1)); // parent p1
        // ------------------------- child -----------------------------------------------------------------------------
        RowDef child = ais.getTable(tableName("child")).rowDef();
        UserTable c = (UserTable) child.table();
        Assert.assertEquals(2, parent.getHKeyDepth()); // child ordinal, child row counter
        checkHKey(c.hKey(),
                  p, c, "p1",
                  c, c, Column.AKIBAN_PK_NAME);
        // c2, c1 index. Row is (c.c2, c.c1, c.p1, c.HIDDEN_PK)
        index = c.getIndex("c2_c1");
        Assert.assertTrue(!index.isPrimaryKey());
        Assert.assertTrue(!index.isUnique());
        rowComp = index.indexRowComposition();
        Assert.assertEquals(1, rowComp.getFieldPosition(0)); // child.c2
        Assert.assertEquals(0, rowComp.getFieldPosition(1)); // child.c1
        indexToHKey = index.indexToHKey();
        Assert.assertEquals(parent.getOrdinal(), indexToHKey.getOrdinal(0)); // parent ordinal
        Assert.assertEquals(2, indexToHKey.getIndexRowPosition(1)); // child p1
        Assert.assertEquals(child.getOrdinal(), indexToHKey.getOrdinal(2)); // child ordinal
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
