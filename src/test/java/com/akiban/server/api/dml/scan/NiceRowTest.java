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

package com.akiban.server.api.dml.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.rowdata.SchemaFactory;
import org.junit.Test;

public final class NiceRowTest {
    @Test
    public void toRowDataBasic() throws Exception
    {
        RowDef rowDef = createRowDef(2);

        Object[] objects = new Object[2];
        objects[0] = 5;
        objects[1] = "Bob";

        RowData rowData = create(rowDef, objects);

        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        // Why -1: because an __akiban_pk column gets added
        assertEquals("fields count", 2, newRow.getFields().size() - 1);
        assertEquals("field[0]", 5L, newRow.get(0));
        assertEquals("field[1]", "Bob", newRow.get(1));

        compareRowDatas(rowData, newRow.toRowData());
    }

    @Test
    public void toRowDataLarge() throws Exception
    {
        final int NUM = 30;
        RowDef rowDef = createRowDef(NUM);

        Object[] objects = new Object[NUM];
        objects[0] = 15;
        objects[1] = "Robert";
        for (int i=2; i < NUM; ++i) {
            objects[i] = i + 1000;
        }

        RowData rowData = create(rowDef, objects);

        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        // Why -1: because an __akiban_pk column gets added
        assertEquals("fields count", NUM, newRow.getFields().size() - 1);
        assertEquals("field[0]", 15L, newRow.get(0));
        assertEquals("field[1]", "Robert", newRow.get(1));
        for (int i=2; i < NUM; ++i) {
            long expected = i + 1000;
            assertEquals("field[1]", expected, newRow.get(i));
        }

        compareRowDatas(rowData, newRow.toRowData());
    }

    @Test
    public void toRowDataSparse() throws Exception
    {
        final int NUM = 30;
        RowDef rowDef = createRowDef(NUM);

        Object[] objects = new Object[NUM];
        objects[0] = 15;
        objects[1] = "Robert";
        int nulls = 0;
        for (int i=2; i < NUM; ++i) {
            if ( (i % 3) == 0) {
                ++nulls;
            }
            else {
                objects[i] = i + 1000;
            }
        }
        assertTrue("nulls==0", nulls > 0);

        RowData rowData = create(rowDef, objects);

        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        // Why -1: because an __akiban_pk column gets added
        assertEquals("fields count", NUM, newRow.getFields().size() - 1);
        assertEquals("field[0]", 15L, newRow.get(0));
        assertEquals("field[1]", "Robert", newRow.get(1));
        for (int i=2; i < NUM; ++i) {
            Long expected = (i % 3) == 0 ? null : i + 1000L;
            assertEquals("field[1]", expected, newRow.get(i));
        }

        compareRowDatas(rowData, newRow.toRowData());
    }

    @Test
    public void testEquality() {
        TreeMap<Integer,NiceRow> mapOne = new TreeMap<Integer, NiceRow>();
        TreeMap<Integer,NiceRow> mapTwo = new TreeMap<Integer, NiceRow>();
        NiceRow rowOne = new NiceRow(1, (RowDef)null);
        rowOne.put(0, Long.valueOf(0l));
        rowOne.put(1, "hello world");
        mapOne.put(0, rowOne);

        NiceRow rowTwo = new NiceRow(1, (RowDef)null);
        rowTwo.put(0, Long.valueOf(0l));
        rowTwo.put(1, "hello world");
        mapTwo.put(0, rowTwo);

        assertEquals("rows", rowOne, rowTwo);
        assertEquals("maps", mapOne, mapTwo);
    }

    @Test
    public void toRowDataOneByteUTF8() throws Exception {
        final int BYTE_COUNT = 0x7F;
        byte[] bytes = new byte[BYTE_COUNT];
        for(int i = 0; i < BYTE_COUNT; ++i) {
            bytes[i] = (byte)i;
        }
        String str = new String(bytes, "utf8");

        String ddl = "create table test.t(id int not null primary key, v varchar(255) character set utf8)";
        RowDef rowDef = SCHEMA_FACTORY.rowDefCache(ddl).getRowDef("test", "t");

        Object[] objects = { 1L, str };
        RowData rowData = create(rowDef, objects);
        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        assertEquals("fields count", 2, newRow.getFields().size());
        assertEquals("field[0]", 1L, newRow.get(0));
        assertEquals("field[1]", str, newRow.get(1));
        assertEquals("filed[1] charset", "utf8", rowDef.getFieldDef(1).column().getCharsetAndCollation().charset());

        compareRowDatas(rowData, newRow.toRowData());
    }

    // bug1057016
    @Test
    public void toRowDataStringWithSurrogatePairs() throws Exception {
        final String TEST_STR = "abc \ud83d\ude0d def";

        assertEquals("string length", 10, TEST_STR.length());
        assertEquals("char 4 high surrogate", true, Character.isHighSurrogate(TEST_STR.charAt(4)));
        assertEquals("char 5 low surrogate", true, Character.isLowSurrogate(TEST_STR.charAt(5)));
        assertEquals("utf8 byte length", 12, TEST_STR.getBytes("UTF-8").length);

        String ddl = "create table test.t(id int not null primary key, v varchar(32) character set utf8)";
        RowDef rowDef = SCHEMA_FACTORY.rowDefCache(ddl).getRowDef("test", "t");

        Object[] objects = { 1L, TEST_STR };
        RowData rowData = create(rowDef, objects);
        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        assertEquals("fields count", 2, newRow.getFields().size());
        assertEquals("field[0]", 1L, newRow.get(0));
        assertEquals("field[1]", TEST_STR, newRow.get(1));
        assertEquals("filed[1] charset", "utf8", rowDef.getFieldDef(1).column().getCharsetAndCollation().charset());

        compareRowDatas(rowData, newRow.toRowData());
    }

    private static byte[] bytes() {
        return new byte[1024];
    }

    private static RowDef createRowDef(int totalColumns) throws Exception {
        assertTrue("bad totalColumns=" + totalColumns, totalColumns >= 2);
        String[] ddl = new String[totalColumns + 2];
        int i = 0;
        ddl[i++] = "create table test_table(";
        ddl[i++] = "id int";
        ddl[i++] = ", name varchar(128)";
        for (int c = 2; c < totalColumns; c++) {
            ddl[i++] = String.format(", field_%s int", c);
        }
        ddl[i] = ");";
        RowDefCache rowDefCache = SCHEMA_FACTORY.rowDefCache(ddl);
        return rowDefCache.getRowDef("test_schema", "test_table");
    }

    private RowData create(RowDef rowDef, Object[] objects) {
        RowData rowData = new RowData(bytes());
        rowData.createRow(rowDef, objects);

        assertEquals("start", 0, rowData.getBufferStart());
        assertEquals("end and length", rowData.getBufferEnd(), rowData.getBufferLength());
        return rowData;
    }

    private void compareRowDatas(RowData expected, RowData actual) {
        if (expected == actual) {
            return;
        }

        List<Byte> expectedBytes = byteListFor(expected);
        List<Byte> actualBytes = byteListFor(actual);
        assertEquals("bytes", expectedBytes, actualBytes);
    }

    private List<Byte> byteListFor(RowData rowData) {
        byte[] bytes = rowData.getBytes();
        assertNotNull("RowData bytes[] null", bytes);
        assertTrue("start < 0: " + rowData.getRowStart(), rowData.getRowStart() >= 0);
        assertTrue("end out of range: " + rowData.getRowEnd(), rowData.getRowEnd() <= bytes.length);

        List<Byte> bytesList = new ArrayList<Byte>();
        for (int i=rowData.getBufferStart(), MAX=rowData.getRowEnd(); i < MAX; ++i) {
            bytesList.add(bytes[i]);
        }
        return bytesList;
    }

    private static final SchemaFactory SCHEMA_FACTORY = new SchemaFactory("test_schema");
}
