package com.akiban.cserver;

import static junit.framework.Assert.assertSame;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import junit.framework.Assert;

import org.junit.Test;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.UserTable;

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
            "    key(e, d), ",
            "    unique key(d, b)",
            ");"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        RowDef test = rowDefCache.getRowDef(tableName("test"));
        UserTable t = (UserTable)test.table();
        assertEquals(2, test.getHKeyDepth()); // test ordinal, test row counter
        checkHKey(t.hKey(), t, t, Column.AKIBAN_PK_NAME);
        IndexDef index;
        IndexDef.H2I[] indexKeyFields;
        IndexDef.I2H[] hKeyFields;
        // e, d index
        index = index(test, "e", "d");
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        indexKeyFields = index.indexKeyFields();
        assertEquals(4, indexKeyFields[0].fieldIndex()); // test.e
        assertEquals(3, indexKeyFields[1].fieldIndex()); // test.d
        assertEquals(1, indexKeyFields[2].hKeyLoc()); // Akiban PK
        hKeyFields = index.hkeyFields();
        assertEquals(test.getOrdinal(), hKeyFields[0].ordinal()); // test ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // test row counter
        // d, b index
        index = index(test, "d", "b");
        assertTrue(!index.isPkIndex());
        assertTrue(index.isUnique());
        indexKeyFields = index.indexKeyFields();
        assertEquals(3, indexKeyFields[0].fieldIndex()); // test.d
        assertEquals(1, indexKeyFields[1].fieldIndex()); // test.b
        assertEquals(1, indexKeyFields[2].hKeyLoc()); // Akiban PK
        hKeyFields = index.hkeyFields();
        assertEquals(test.getOrdinal(), hKeyFields[0].ordinal()); // test ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // Akiban PK
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
            "    key(c2, c1)",
            ");"
        };
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        IndexDef index;
        IndexDef.H2I[] indexKeyFields;
        IndexDef.I2H[] hKeyFields;
        // ------------------------- parent ----------------------------------------------------------------------------
        RowDef parent = rowDefCache.getRowDef(tableName("parent"));
        UserTable p = (UserTable) parent.table();
        assertEquals(2, parent.getHKeyDepth()); // parent ordinal, p1
        checkHKey(p.hKey(), p, p, "p1");
        // PK index
        index = index(parent, "p1");
        assertTrue(index.isPkIndex());
        assertTrue(index.isUnique());
        assertTrue(index.isHKeyEquivalent());
        indexKeyFields = index.indexKeyFields();
        assertEquals(0, indexKeyFields[0].fieldIndex()); // parent.p1
        hKeyFields = index.hkeyFields();
        assertEquals(parent.getOrdinal(), hKeyFields[0].ordinal()); // parent ordinal
        assertEquals(0, hKeyFields[1].indexKeyLoc()); // parent p1
        // ------------------------- child -----------------------------------------------------------------------------
        RowDef child = rowDefCache.getRowDef(tableName("child"));
        UserTable c = (UserTable) child.table();
        assertEquals(2, parent.getHKeyDepth()); // child ordinal, child row counter
        checkHKey(c.hKey(),
                  p, c, "p1",
                  c, c, Column.AKIBAN_PK_NAME);
        // c2, c1 index
        index = index(child, "c2", "c1");
        assertTrue(!index.isPkIndex());
        assertTrue(!index.isUnique());
        assertTrue(!index.isHKeyEquivalent());
        indexKeyFields = index.indexKeyFields();
        assertEquals(1, indexKeyFields[0].fieldIndex()); // child.c2
        assertEquals(0, indexKeyFields[1].fieldIndex()); // child.c1
        hKeyFields = index.hkeyFields();
        assertEquals(parent.getOrdinal(), hKeyFields[0].ordinal()); // parent ordinal
        assertEquals(2, hKeyFields[1].fieldIndex()); // child p1
        assertEquals(child.getOrdinal(), hKeyFields[2].ordinal()); // child ordinal
        assertEquals(2, hKeyFields[1].indexKeyLoc()); // child row counter
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

    // Copied from AISTest, generalized for pkles tables
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
