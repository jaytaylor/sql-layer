/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;
import org.junit.Test;
import java.util.*;
import java.io.*;
import java.nio.ByteBuffer;

import com.akiban.cserver.*;
import com.akiban.vstore.ColumnArrayGenerator;
import com.akiban.vstore.ColumnDescriptor;
import com.akiban.vstore.VMeta;
import com.akiban.ais.ddl.*;
import com.akiban.ais.model.*;

/**
 * @author percent
 * 
 */
public class VCollectorTest {

    private final static String VCOLLECTOR_DDL = "src/test/resources/vcollector_test.ddl";
    private final static String MANY_DDL = "src/test/resources/many_columns.ddl";
    private final static String VCOLLECTOR_TEST_DATADIR = "target/vcollector_test_data/";
    private static RowDefCache rowDefCache;
    private int rows = 15;
    private int rowSize = 0;
    private RowDef testRowDef = null;
    private ArrayList<ColumnDescriptor> columnDes = new ArrayList<ColumnDescriptor>();
    private ArrayList<ColumnArrayGenerator> columns = new ArrayList<ColumnArrayGenerator>();
    private ArrayList<ArrayList<byte[]>> encodedColumns = new ArrayList<ArrayList<byte[]>>();
    private ArrayList<RowData> rowData = new ArrayList<RowData>();
    private VMeta meta;

    public void generateEncodedData(RowDef rowDef, BitSet projection)
            throws Exception {

        File directory = new File(VCOLLECTOR_TEST_DATADIR);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new Exception();
            }
        }

        String schemaName = rowDef.getSchemaName();
        String tableName = rowDef.getTableName();
        String prefix = VCOLLECTOR_TEST_DATADIR + schemaName + tableName;

        FieldDef[] fields = rowDef.getFieldDefs();
        assert fields.length == rowDef.getFieldCount();
        rowSize = 0;

        for (int i = 0; i < fields.length; i++) {
            assert fields[i].isFixedSize() == true;
            if (projection.get(i)) {
                rowSize += fields[i].getMaxStorageSize();
                columns.add(new ColumnArrayGenerator(prefix
                        + fields[i].getName(), 1337 + i, fields[i]
                        .getMaxStorageSize(), rows));
                encodedColumns.add(new ArrayList<byte[]>());
            }
        }

        // God, why can't this be easier?

        for (int i = 0; i < rows; i++) {
            Object[] aRow = new Object[rowDef.getFieldCount()];
            RowData aRowData = new RowData(new byte[rowSize
                    + RowData.MINIMUM_RECORD_LENGTH
                    + ((rowDef.getFieldCount() % 8) == 0 ? rowDef
                            .getFieldCount() / 8
                            : rowDef.getFieldCount() / 8 + 1)]);

            for (int j = 0, k = 0; j < rowDef.getFieldCount(); j++) {
                if (projection.get(j)) {
                    byte[] b = columns.get(k).generateMemoryFile(1).get(0);
                    assert b.length == 4;
                    k++;
                    int rawFieldInt = ((int) (b[0] & 0xff) << 24)
                            | ((int) (b[1] & 0xff) << 16)
                            | ((int) (b[2] & 0xff) << 8) | (int) b[3] & 0xff;
                    aRow[j] = rawFieldInt;
                } else {
                    aRow[j] = null;
                }
            }

            aRowData.createRow(rowDef, aRow);
            rowData.add(aRowData);

            for (int j = 0, k = 0; j < rowDef.getFieldCount(); j++) {
                if (projection.get(j)) {
                    long offset_width = rowDef.fieldLocation(aRowData, j);
                    int offset = (int) offset_width;
                    int width = (int) (offset_width >>> 32);
                    byte[] bytes = aRowData.getBytes();
                    byte[] field = new byte[width];
                    // System.out.println("offset = "+offset+", width = "+width);
                    for (int l = 0; l < width; l++) {
                        field[l] = bytes[offset + l];
                    }
                    encodedColumns.get(k).add(field);
                    k++;
                }
            }
        }

        for (int i = 0, j = 0; i < fields.length; i++) {
            try {
                if (projection.get(i)) {
                    assert fields[i] != null;
                    columns.get(j).writeEncodedColumn(encodedColumns.get(j));
                    j++;
                    columnDes.add(new ColumnDescriptor(VCOLLECTOR_TEST_DATADIR,
                            schemaName, tableName, fields[i].getName(), rowDef
                                    .getRowDefId(), i, fields[i]
                                    .getMaxStorageSize(), rows));
                }
                // columnArray.add(new ColumnArray(new File(prefix
                // + fields[i].getName())));
                // columnDes.get(i).setColumnArray(columnArray.get(i));
            } catch (FileNotFoundException e) {
                System.out.println("FILE NOT FOUND");
                // e.printStackTrace();
                assert false;
            } catch (IOException e) {
                System.out.println("IO EXCEPTION");
                // e.printStackTrace();
                assert false;
            }

        }
        meta = new VMeta(columnDes);
    }

    public void setupDatabase() throws Exception {

        rowDefCache = new RowDefCache();

        AkibaInformationSchema ais = null;
        try {
            ais = new DDLSource().buildAIS(VCOLLECTOR_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            fail("ais gen failed");
            return;
        }
        rowDefCache.setAIS(ais);
    }

    public byte[] setupBitMap(BitSet projection, Random rand, int fieldCount) {
        int mapSize = fieldCount / 8;
        if (fieldCount % 8 != 0) {
            mapSize++;
        }

        byte[] bitMap = new byte[mapSize];
        bitMap[0] |= 1;
        projection.set(0);

        for (int j = 1, offset = 0; j < fieldCount; j++) {
            projection.set(j, rand.nextBoolean());
            if (projection.get(j)) {
                bitMap[offset] |= 1 << j % 8;
            }
            if ((j + 1) % 8 == 0) {
                offset++;
            }

        }
        return bitMap;
    }

    @Test
    public void testVCollector() throws Exception {

        try {
            setupDatabase();

            List<RowDef> rowDefs = rowDefCache.getRowDefs();
            Iterator<RowDef> i = rowDefs.iterator();
            while (i.hasNext()) {

                RowDef rowDef = i.next();
                if (!rowDef.isGroupTable()) {
                    continue;
                }

                testRowDef = rowDef;

                int mapSize = testRowDef.getFieldCount() / 8;
                if (testRowDef.getFieldCount() % 8 != 0) {
                    mapSize++;
                }

                byte[] columnBitMap = new byte[mapSize];
                BitSet projection = new BitSet(mapSize);
                for (int j = 0; j < testRowDef.getFieldCount(); j++) {
                    columnBitMap[j / 8] |= 1 << (j % 8);
                    projection.set(j, true);
                }

                columnDes = new ArrayList<ColumnDescriptor>();
                columns = new ArrayList<ColumnArrayGenerator>();
                encodedColumns = new ArrayList<ArrayList<byte[]>>();
                rowData = new ArrayList<RowData>();
                meta = null;

                generateEncodedData(testRowDef, projection);
                // System.out.println("RowDef id = "+testRowDef.getRowDefId()
                // +" table name = "+ testRowDef.getTableName());

                VCollector vc = new VCollector(meta, rowDefCache, testRowDef
                        .getRowDefId(), columnBitMap);

                ByteBuffer buffer = ByteBuffer.allocate((rowSize
                        + RowData.MINIMUM_RECORD_LENGTH + mapSize)
                        * rows);
                /*
                 * System.out.println(">>>>>>>> tableId: "+testRowDef.getTableName
                 * () +", "+ testRowDef.getRowDefId() +", " +
                 * testRowDef.getParentRowDefId());
                 */
                if (testRowDef.getRowDefId() == 1003) {
                    // System.out.println("----> debugToString: "+testRowDef.debugToString());
                    // System.out.println("----> parentJoin fields: "+testRowDef.getParentJoinFields());
                    // System.out.println("----> rowType: "+testRowDef.getRowType());
                    // System.out.println("----> isGroup: "+testRowDef.isGroupTable());
                    // System.out.println("----> getUserTableRowDefs: "+testRowDef.getUserTableRowDefs());
                    // System.out.println("----> groupRowDef: "+testRowDef.getGroupRowDefId());

                    boolean copied = vc.collectNextRow(buffer);
                    assertTrue(copied);
                    assertFalse(vc.hasMore());
                    Iterator<RowData> j = rowData.iterator();
                    while (j.hasNext()) {
                        RowData row = j.next();
                        byte[] expected = row.getBytes();
                        byte[] actual = new byte[expected.length];
                        buffer.get(actual);
                        assertArrayEquals(expected, actual);
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("ERROR because " + e.getMessage());
            e.printStackTrace();
            fail("vcollector build failed");
        }
        /*
         * System.out.println("----------------------------------------------");
         * byte b = 0; b |= ((1 << 0));
         * System.out.println("b = "+Integer.toHexString(b)); b |= ((1 << 1));
         * System.out.println("b = "+Integer.toHexString(b)); b |= ((1 << 2) &
         * 0xff); System.out.println("b = "+Integer.toHexString(b)); b |= ((1 <<
         * 3) & 0xff); System.out.println("b = "+Integer.toHexString(b)); b |=
         * ((1 << 4) & 0xff); System.out.println("b = "+Integer.toHexString(b));
         * b |= ((1 << 5) & 0xff);
         * System.out.println("b = "+Integer.toHexString(b)); b |= ((1 << 6) &
         * 0xff); System.out.println("b = "+Integer.toHexString(b)); b |= ((1 <<
         * 7) & 0xff); System.out.println("b = "+Integer.toHexString(b));
         */

    }

    @Test
    public void testCollectNextRow()
        throws Exception
    {
        try {
            setupDatabase();
            List<RowDef> rowDefs = rowDefCache.getRowDefs();
            Iterator<RowDef> iter = rowDefs.iterator();
            while (iter.hasNext()) {
                RowDef rowDef = iter.next();
                if (! rowDef.isGroupTable()) {
                    continue;
                }

                int mapSize = rowDef.getFieldCount() / 8;
                if (rowDef.getFieldCount() % 8 != 0) {
                    mapSize++;
                }

                byte[] columnBitMap = new byte[mapSize];
                BitSet projection = new BitSet(mapSize);
                for (int j = 0; j < rowDef.getFieldCount(); j++) {
                    columnBitMap[j / 8] |= 1 << (j % 8);
                    projection.set(j, true);
                }

                columnDes = new ArrayList<ColumnDescriptor>();
                columns = new ArrayList<ColumnArrayGenerator>();
                encodedColumns = new ArrayList<ArrayList<byte[]>>();
                rowData = new ArrayList<RowData>();
                meta = null;

                generateEncodedData(rowDef, projection);

                VCollector vc = new VCollector(meta, 
                                               rowDefCache, 
                                               rowDef.getRowDefId(), 
                                               columnBitMap);

                /* we want to retrieve 5 row chunks at a time from the VCollector */
                int rowChunk = 5;
                int currentRow = 0; /* used as an index into the rowData array */
                int totalRowSize = rowSize + RowData.MINIMUM_RECORD_LENGTH + mapSize;
                int bufferSize = totalRowSize * rowChunk;
                ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
                if (rowDef.getRowDefId() == 1003) {
                    int i = 0;
                    while (i < rows) {
                        if (vc.hasMore()) {
                            buffer.clear();
                            vc.collectNextRow(buffer);
                            /* iterate through the rows placed in the ByteBuffer */
                            int rowIter = 0;
                            while (rowIter < rowChunk) {
                                byte[] actual = new byte[totalRowSize];
                                buffer.get(actual , 0, totalRowSize);
                                RowData row = (RowData) rowData.get(currentRow);
                                byte[] expected = row.getBytes();
                                assertArrayEquals(expected, actual);
                                currentRow++;
                                rowIter++;
                            }
                            /* go on to the next row chunk */
                            i += rowChunk;
                        } else {
                            break; /* we have no more rows to get */
                        }
                    }
                    assertEquals(i, rows);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("testCollectNextRow test failed");
        }
    }

    @Test
    public void testProjection() throws Exception {
/*
        Random r = new Random(1337);
        rowDefCache = null;
        testRowDef = null;
        setupDatabase();
        for (int h = 0; h < 1337; h++) {

            try {

                List<RowDef> rowDefs = rowDefCache.getRowDefs();
                Iterator<RowDef> m = rowDefs.iterator();
                while (m.hasNext()) {
                    RowDef rowDef = m.next();
                    if (!rowDef.isGroupTable()) {
                        continue;
                    }
                    testRowDef = rowDef;

                    columnDes = new ArrayList<ColumnDescriptor>();
                    columns = new ArrayList<ColumnArrayGenerator>();
                    encodedColumns = new ArrayList<ArrayList<byte[]>>();
                    rowData = new ArrayList<RowData>();
                    meta = null;

                    int mapSize = testRowDef.getFieldCount() / 8;
                    if (testRowDef.getFieldCount() % 8 != 0) {
                        mapSize++;
                    }

                    byte[] columnBitMap = new byte[mapSize];
                    BitSet projection = new BitSet(mapSize);
                    boolean none = true;
                    for (int i = 0; i < testRowDef.getFieldCount(); i++) {
                        if (r.nextBoolean()
                                || (none == true && i + 1 == testRowDef
                                        .getFieldCount())) {
                            projection.set(i, true);
                            columnBitMap[i / 8] |= 1 << (i % 8);
                            none = false;
                        }
                    }
                    generateEncodedData(testRowDef, projection);

                    VCollector vc = new VCollector(meta, rowDefCache,
                            testRowDef.getRowDefId(), columnBitMap);
                    ByteBuffer buffer = ByteBuffer.allocate((rowSize
                            + RowData.MINIMUM_RECORD_LENGTH + mapSize)
                            * rows);
                    boolean copied = vc.collectNextRow(buffer);
                    assertTrue(copied);
                    assertFalse(vc.hasMore());
                    Iterator<RowData> j = rowData.iterator();
                    while (j.hasNext()) {
                        RowData row = j.next();
                        byte[] expected = row.getBytes();
                        byte[] actual = new byte[expected.length];
                        buffer.get(actual);
                        assertArrayEquals(expected, actual);
                    }
                }
            } catch (Exception e) {
                System.out.println("ERROR because " + e.getMessage());
                e.printStackTrace();
                fail("vcollector build failed");
            }

        }*/
    }

    @Test
    public void testGetProjection() throws Exception {

        setupDatabase();
        Random rand = new Random(31337);
        List<RowDef> rowDefs = rowDefCache.getRowDefs();
        Iterator<RowDef> i = rowDefs.iterator();

        while (i.hasNext()) {
            RowDef rowDef = i.next();
            if (!rowDef.isGroupTable()) {
                continue;
            }

            BitSet projection = new BitSet(rowDef.getFieldCount());
            projection.clear();
            byte[] bitMap = setupBitMap(projection, rand, rowDef
                    .getFieldCount());

            generateEncodedData(rowDef, projection);

            VCollector vc = new VCollector(meta, rowDefCache, rowDef
                    .getRowDefId(), bitMap);

            assert vc.getProjection().equals(projection);
            assertTrue(vc.getProjection().equals(projection));
        }
    }

    @Test
    public void testGetUserTables() throws Exception {

        setupDatabase();
        Random rand = new Random(31337);
        List<RowDef> rowDefs = rowDefCache.getRowDefs();
        Iterator<RowDef> i = rowDefs.iterator();

        while (i.hasNext()) {

            RowDef rowDef = i.next();
            if (!rowDef.isGroupTable()) {
                continue;
            }

            BitSet projection = new BitSet(rowDef.getFieldCount());
            projection.clear();
            byte[] bitMap = setupBitMap(projection, rand, rowDef
                    .getFieldCount());

            generateEncodedData(rowDef, projection);

            VCollector vc = new VCollector(meta, rowDefCache, rowDef
                    .getRowDefId(), bitMap);

            ArrayList<RowDef> tables = vc.getUserTables();
            assertTrue(tables.size() > 0);
            // XXX - implement me.
        }
    }

    @Test
    public void testManyColumns() throws Exception {
        rowDefCache = new RowDefCache();

        AkibaInformationSchema ais = null;
        try {
            ais = new DDLSource().buildAIS(MANY_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            fail("ais gen failed");
            return;
        }
        rowDefCache.setAIS(ais);
        try {
            List<RowDef> rowDefs = rowDefCache.getRowDefs();
            Iterator<RowDef> i = rowDefs.iterator();
            while (i.hasNext()) {

                RowDef rowDef = i.next();
                if (!rowDef.isGroupTable()) {
                    continue;
                }

                testRowDef = rowDef;

                int mapSize = testRowDef.getFieldCount() / 8;
                if (testRowDef.getFieldCount() % 8 != 0) {
                    mapSize++;
                }

                byte[] columnBitMap = new byte[mapSize];
                BitSet projection = new BitSet(mapSize);
                for (int j = 0; j < testRowDef.getFieldCount(); j++) {
                    columnBitMap[j / 8] |= 1 << (j % 8);
                    projection.set(j, true);
                }

                columnDes = new ArrayList<ColumnDescriptor>();
                columns = new ArrayList<ColumnArrayGenerator>();
                encodedColumns = new ArrayList<ArrayList<byte[]>>();
                rowData = new ArrayList<RowData>();
                meta = null;

                generateEncodedData(testRowDef, projection);

                VCollector vc = new VCollector(meta, rowDefCache, testRowDef
                        .getRowDefId(), columnBitMap);

                ByteBuffer buffer = ByteBuffer.allocate((rowSize
                        + RowData.MINIMUM_RECORD_LENGTH + mapSize)
                        * rows);

                boolean copied = vc.collectNextRow(buffer);
                assertTrue(copied);
                assertFalse(vc.hasMore());
                Iterator<RowData> j = rowData.iterator();
                while (j.hasNext()) {
                    RowData row = j.next();
                    byte[] expected = row.getBytes();
                    byte[] actual = new byte[expected.length];
                    buffer.get(actual);
                    assertArrayEquals(expected, actual);
                }
            }
        } catch (Exception e) {
            System.out.println("ERROR because " + e.getMessage());
            e.printStackTrace();
            fail("vcollector build failed");
        }
    }

}
