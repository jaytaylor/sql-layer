package com.akiban.cserver.api.dml.scan;

import com.akiban.ais.model.Types;
import com.akiban.cserver.FieldDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.api.common.ColumnId;
import com.akiban.cserver.api.common.TableId;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.junit.Assert.*;

public final class NiceRowTest {
    @Test
    public void toRowDataBasic() {
        RowDef rowDef = createRowDef(2);

        Object[] objects = new Object[2];
        objects[0] = 5;
        objects[1] = "Bob";

        RowData rowData = create(rowDef, objects);

        NewRow newRow = NiceRow.fromRowData(rowData, rowDef);

        assertEquals("fields count", 2, newRow.getFields().size());
        assertEquals("field[0]", 5L, newRow.get(0));
        assertEquals("field[1]", "Bob", newRow.get(1));

        compareRowDatas(rowData, newRow.toRowData(rowDef));
    }

    @Test
    public void toRowDataLarge() {
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

        assertEquals("fields count", NUM, newRow.getFields().size());
        assertEquals("field[0]", 15L, newRow.get(0));
        assertEquals("field[1]", "Robert", newRow.get(1));
        for (int i=2; i < NUM; ++i) {
            long expected = i + 1000;
            assertEquals("field[1]", expected, newRow.get(i));
        }

        compareRowDatas(rowData, newRow.toRowData(rowDef));
    }

    @Test
    public void toRowDataSparse() {
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

        assertEquals("fields count", NUM - nulls, newRow.getFields().size());
        assertEquals("field[0]", 15L, newRow.get(0));
        assertEquals("field[1]", "Robert", newRow.get(1));
        for (int i=2; i < NUM; ++i) {
            Long expected = (i % 3) == 0 ? null : Long.valueOf(i + 1000);
            assertEquals("field[1]", expected, newRow.get(i));
        }

        compareRowDatas(rowData, newRow.toRowData(rowDef));
    }

    @Test
    public void testEquality() {
        TreeMap<Integer,NiceRow> mapOne = new TreeMap<Integer, NiceRow>();
        TreeMap<Integer,NiceRow> mapTwo = new TreeMap<Integer, NiceRow>();
        NiceRow rowOne = new NiceRow(TableId.of(1));
        rowOne.put(new ColumnId(0), Long.valueOf(0l));
        rowOne.put(new ColumnId(1), "hello world");
        mapOne.put(0, rowOne);

        NiceRow rowTwo = new NiceRow(TableId.of(1));
        rowTwo.put(new ColumnId(0), Long.valueOf(0l));
        rowTwo.put(new ColumnId(1), "hello world");
        mapTwo.put(0, rowTwo);

        assertEquals("rows", rowOne, rowTwo);
        assertEquals("maps", mapOne, mapTwo);
    }

    private static byte[] bytes() {
        return new byte[1024];
    }

    private static RowDef createRowDef(int totalColumns) {
        assertTrue("bad totalColumns=" + totalColumns, totalColumns >= 2);
        FieldDef[] fields = new FieldDef[totalColumns];
        int i = 0;
        fields[i++] = new FieldDef("id", Types.INT);
        fields[i++] = new FieldDef("name", Types.VARCHAR, 128, 1, 129L, null);
        while(i < totalColumns) {
            fields[i++] = new FieldDef("field_"+i, Types.INT);
        }

        return new RowDef(1, fields);
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
}
