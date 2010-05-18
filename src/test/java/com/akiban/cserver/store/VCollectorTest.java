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
import com.akiban.vstore.ColumnArray;
import com.akiban.vstore.ColumnArrayGenerator;
import com.akiban.vstore.ColumnDescriptor;
import com.akiban.ais.ddl.*;
import com.akiban.ais.model.*;

/**
 * @author percent
 * 
 */
public class VCollectorTest {

    private final static String VCOLLECTOR_DDL = "src/test/resources/vcollector_test.ddl";
    private final static String VCOLLECTOR_TEST_DATADIR = "src/test/resources/vcollector_test_data/";
    private static RowDefCache rowDefCache;
    private int rows = 15;
    private int rowSize = 0;
    private RowDef testRowDef = null;
    private List<ColumnDescriptor> columnDes = new ArrayList<ColumnDescriptor>();
    private ArrayList<ColumnArray> columnArray = new ArrayList<ColumnArray>();
    private ArrayList<ColumnArrayGenerator> columns = new ArrayList<ColumnArrayGenerator>();
    private ArrayList<ArrayList<byte[]>> encodedColumns = new ArrayList<ArrayList<byte[]>>();
    private ArrayList<RowData> rowData = new ArrayList<RowData>();

    public void generateEncodedData(RowDef rowDef) throws Exception {

        File directory = new File(VCOLLECTOR_TEST_DATADIR);
        if (!directory.exists()) {
            if (!directory.mkdir()) {
                throw new Exception();
            }
        }

        String schemaName = rowDef.getSchemaName();
        String tableName = rowDef.getTableName();
        String prefix = VCOLLECTOR_TEST_DATADIR + schemaName + "." + tableName
                + ".";

        FieldDef[] fields = rowDef.getFieldDefs();
        assert fields.length == rowDef.getFieldCount();
        rowSize = 0;

        for (int i = 0; i < fields.length; i++) {
            assert fields[i].isFixedSize() == true;
            rowSize += fields[i].getMaxStorageSize();
            columns.add(new ColumnArrayGenerator(prefix + fields[i].getName(),
                    1337 + i, fields[i].getMaxStorageSize(), rows));
            encodedColumns.add(new ArrayList<byte[]>());
        }

        // God, why can't this be easier?

        for (int i = 0; i < rows; i++) {
            Object[] aRow = new Object[rowDef.getFieldCount()];
            RowData aRowData = new RowData(new byte[rowSize
                    + RowData.MINIMUM_RECORD_LENGTH + 1]);

            for (int j = 0; j < rowDef.getFieldCount(); j++) {
                byte[] b = columns.get(j).generateMemoryFile(1).get(0);
                assert b.length == 4;

                int rawFieldInt = ((int) (b[0] & 0xff) << 24)
                        | ((int) (b[1] & 0xff) << 16)
                        | ((int) (b[2] & 0xff) << 8) | (int) b[3] & 0xff;
                aRow[j] = rawFieldInt;
            }
            aRowData.createRow(rowDef, aRow);
            rowData.add(aRowData);

            for (int j = 0; j < rowDef.getFieldCount(); j++) {
                long offset_width = rowDef.fieldLocation(aRowData, j);
                int offset = (int) offset_width;
                int width = (int) (offset_width >>> 32);
                byte[] bytes = aRowData.getBytes();
                byte[] field = new byte[width];
                // System.out.print("offset = "+offset+", width = "+width);
                for (int k = 0; k < width; k++) {
                    field[k] = bytes[offset + k];
                }
                encodedColumns.get(j).add(field);
            }
        }

        for (int i = 0; i < fields.length; i++) {
            try {
                columns.get(i).writeEncodedColumn(encodedColumns.get(i));
                columnDes.add(new ColumnDescriptor(schemaName, tableName,
                        fields[i].getName(), fields[i].getMaxStorageSize(),
                        rows));
                columnArray.add(new ColumnArray(new File(prefix
                        + fields[i].getName())));
                columnDes.get(i).setColumnArray(columnArray.get(i));
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
    }

    public void setupDatabase() throws Exception {

        rowDefCache = new RowDefCache();
        byte[] columns = new byte[1];

        AkibaInformationSchema ais = null;
        try {
            ais = new DDLSource().buildAIS(VCOLLECTOR_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            fail("ais gen failed");
            return;
        }

        rowDefCache.setAIS(ais);
        int rowDefId = 1001;

        List<RowDef> rowDefs = rowDefCache.getRowDefs();
        Iterator<RowDef> i = rowDefs.iterator();
        while (i.hasNext()) {
            RowDef rowDef = i.next();
            // System.out.println("rowDef debugToString = "+
            // rowDef.debugToString() + "<----");
            /*System.out.println("rowDef to string = " + rowDef.toString()
                    + "<---");
            System.out.println("rowDef get ordinal = " + rowDef.getOrdinal()
                    + "<---");
*/
            FieldDef[] fields = rowDef.getFieldDefs();
            int j = 0;
            while (j < fields.length) {
                /*
                System.out.print("field name = " + fields[j].getName());
                System.out.print(", index = " + fields[j].getFieldIndex());
                System.out.print(", size = " + fields[j].getMaxStorageSize());
                System.out
                        .print(", prefix size = " + fields[j].getPrefixSize());
                System.out.print(", encoding = " + fields[j].getEncoding());
                System.out.print(", type = " + fields[j].getType());
                System.out.println(" and is fixed size = "
                        + fields[j].isFixedSize());
                        */
                j++;
            }

            if (rowDef.getRowDefId() == 1001) {
                generateEncodedData(rowDef);
                testRowDef = rowDef;
            }
        }
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
                if ((j + 1) % 8 == 0) {
                    offset++;
                }
            }
        }
        return bitMap;
    }

    @Test
    public void testVCollector() throws Exception {

        try {

            setupDatabase();
            
            int mapSize = testRowDef.getFieldCount()/8;
            if(testRowDef.getFieldCount() % 8 != 0) {
                mapSize++;
            }
            
            byte[] columnBitMap = new byte[mapSize];

            for (int i = 0; i < testRowDef.getFieldCount(); i++) {
                columnBitMap[i/8] |= 1 << (i % 8);
            }
            
            VCollector vc = new VCollector(CServerConfig.unitTestConfig(),
                    rowDefCache, testRowDef.getRowDefId(), columnBitMap);

            vc.setColumnDescriptors(columnDes);
            System.out.println("fieldCount = "+testRowDef.getFieldCount() + "mapSize ="+ mapSize);
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
        } catch (Exception e) {
            System.out.println("ERROR because " + e.getMessage());
            e.printStackTrace();
            fail("vcollector build failed");
        }

    }

    @Test
    public void testGetProjection() throws Exception {
        setupDatabase();
        Random rand = new Random(31337);
        List<RowDef> rowDefs = rowDefCache.getRowDefs();
        Iterator<RowDef> i = rowDefs.iterator();

        while (i.hasNext()) {
            RowDef rowDef = i.next();
            BitSet projection = new BitSet(rowDef.getFieldCount());
            projection.clear();
            byte[] bitMap = setupBitMap(projection, rand, rowDef
                    .getFieldCount());

            VCollector vc = new VCollector(CServerConfig.unitTestConfig(),
                    rowDefCache, rowDef.getRowDefId(), bitMap);

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
            if (rowDef.getGroupRowDefId() == 1004) {

            }
            BitSet projection = new BitSet(rowDef.getFieldCount());
            projection.clear();
            byte[] bitMap = setupBitMap(projection, rand, rowDef
                    .getFieldCount());

            VCollector vc = new VCollector(CServerConfig.unitTestConfig(),
                    rowDefCache, rowDef.getRowDefId(), bitMap);

            ArrayList<RowDef> tables = vc.getUserTables();
            assertTrue(tables.size() > 0);
            // implement me.
        }
    }
}
