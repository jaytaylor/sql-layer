/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;
import org.junit.Test;
import java.util.*; //import java.io.*;
import java.nio.ByteBuffer;

import com.akiban.cserver.*;
import com.akiban.vstore.ColumnArrayGenerator; //import com.akiban.vstore.ColumnArrayGenerator;
//import com.akiban.vstore.IColumnDescriptor;
//import com.akiban.vstore.VMeta;
import com.akiban.ais.ddl.*;
import com.akiban.ais.model.*;

/**
 * @author percent
 * 
 */
public class VCollectorTest {

    private final static String VCOLLECTOR_DDL = "src/test/resources/vcollector_test-1.ddl";
    private final static String MANY_DDL = "src/test/resources/many_columns.ddl";
    private final static String VCOLLECTOR_TEST_DATADIR = "vcollector_test_data/";
    private RowDefCache rowDefCache;
    private AkibaInformationSchema ais;

    public void setupDatabase() throws Exception {

        rowDefCache = new RowDefCache();

        ais = null;
        try {
            ais = new DDLSource().buildAIS(VCOLLECTOR_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            fail("ais gen failed");
            return;
        }
        rowDefCache.setAIS(ais);
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

                if (rowDef.getRowDefId() == 1003) {
                    GroupGenerator dbGen = new GroupGenerator(
                            VCOLLECTOR_TEST_DATADIR, ais, rowDefCache, false, true);
                    dbGen.generateGroup(rowDef, 2);
                    ArrayList<RowData> rowData = dbGen.getRows();
                    //DeltaMonitor dm = new DeltaMonitor();
                    //System.out.println("###############################################");
                    //dbGen.getDeltas().dumpInserts(rowDefCache);
                    //if(true)
                        //return;
                    VCollector vc = new VCollector(dbGen.getMeta(), dbGen.getDeltas(),
                            rowDefCache, rowDef.getRowDefId(), dbGen
                                    .getGroupBitMap());
                    // System.out.println("GroupSize = "+ dbGen.getGroupSize());
                    ByteBuffer buffer = ByteBuffer.allocate(dbGen
                            .getGroupSize());
                    /*
                     * System.out.println(">>>>>>>> tableId: "+testRowDef.getTableName
                     * () +", "+ testRowDef.getRowDefId() +", " +
                     * testRowDef.getParentRowDefId());
                     */

                    // System.out.println("----> debugToString: "+testRowDef.debugToString());
                    // System.out.println("----> parentJoin fields: "+testRowDef.getParentJoinFields());
                    // System.out.println("----> rowType: "+testRowDef.getRowType());
                    // System.out.println("----> isGroup: "+testRowDef.isGroupTable());
                    // System.out.println("----> getUserTableRowDefs: "+testRowDef.getUserTableRowDefs());
                    // System.out.println("----> groupRowDef: "+testRowDef.getGroupRowDefId());
                    // System.out.println(buffer.position());
                    // if(true)
                    // return;
                    boolean copied = vc.collectNextRow(buffer);
                    buffer.position(0);
                    assertTrue(copied);
                    assertFalse(vc.hasMore());
                    int rowCount = 0;
                    Iterator<RowData> j = rowData.iterator();
                    while (j.hasNext()) {
                        RowData row = j.next();
                        byte[] expected = row.getBytes();
                        byte[] actual = new byte[expected.length];
                        buffer.get(actual);
                      /*  
                          System.out.println(" count = "+rowCount++); 
                          int k =0; 
                          while(k < expected.length) {
                              System.out.print(Integer.toHexString(expected[k])+" "); 
                              k++; 
                          } 
                          k = 0;
                          System.out.println(); 
                          while (k < actual.length) {
                              System.out.print(Integer.toHexString(actual[k])+" ");
                              k++; 
                          } 
                          System.out.println();
                        */ 
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
    public void testCollectNextRow() throws Exception {
        /*
         * try { setupDatabase(); List<RowDef> rowDefs =
         * rowDefCache.getRowDefs(); Iterator<RowDef> iter = rowDefs.iterator();
         * while (iter.hasNext()) { RowDef rowDef = iter.next(); if (!
         * rowDef.isGroupTable()) { continue; }
         * 
         * int mapSize = rowDef.getFieldCount() / 8; if (rowDef.getFieldCount()
         * % 8 != 0) { mapSize++; }
         * 
         * byte[] columnBitMap = new byte[mapSize]; BitSet projection = new
         * BitSet(mapSize); for (int j = 0; j < rowDef.getFieldCount(); j++) {
         * columnBitMap[j / 8] |= 1 << (j % 8); projection.set(j, true); }
         * 
         * columnDes = new ArrayList<IColumnDescriptor>(); columns = new
         * ArrayList<ColumnArrayGenerator>(); encodedColumns = new
         * ArrayList<ArrayList<byte[]>>(); rowData = new ArrayList<RowData>();
         * meta = null;
         * 
         * generateEncodedData(rowDef, projection);
         * 
         * VCollector vc = new VCollector(meta, rowDefCache,
         * rowDef.getRowDefId(), columnBitMap);
         * 
         * // we want to retrieve 5 row chunks at a time from the VCollector int
         * rowChunk = 5; int currentRow = 0; // used as an index into the
         * rowData array int totalRowSize = rowSize +
         * RowData.MINIMUM_RECORD_LENGTH + mapSize; int bufferSize =
         * totalRowSize * rowChunk; ByteBuffer buffer =
         * ByteBuffer.allocate(bufferSize); if (rowDef.getRowDefId() == 1003) {
         * int i = 0; while (i < rows) { if (vc.hasMore()) { buffer.clear();
         * vc.collectNextRow(buffer); // iterate through the rows placed in the
         * ByteBuffer int rowIter = 0; while (rowIter < rowChunk) { byte[]
         * actual = new byte[totalRowSize]; buffer.get(actual , 0,
         * totalRowSize); RowData row = (RowData) rowData.get(currentRow);
         * byte[] expected = row.getBytes(); assertArrayEquals(expected,
         * actual); currentRow++; rowIter++; } // go on to the next row chunk i
         * += rowChunk; } else { break; // we have no more rows to get } }
         * assertEquals(i, rows); } } } catch (Exception e) {
         * e.printStackTrace(); fail("testCollectNextRow test failed"); }
         */
    }

    @Test
    public void testProjection() throws Exception {

        Random r = new Random(1337);
        for (int h = 0; h < 133; h++) {
            try {
                setupDatabase();
                List<RowDef> rowDefs = rowDefCache.getRowDefs();
                Iterator<RowDef> i = rowDefs.iterator();
                while (i.hasNext()) {
                    RowDef rowDef = i.next();
                    if (!rowDef.isGroupTable()) {
                        continue;
                    }

                    //if (rowDef.getRowDefId() == 1003) {
                        GroupGenerator dbGen = new GroupGenerator(
                                VCOLLECTOR_TEST_DATADIR, ais, rowDefCache, true, false);
                        dbGen.generateGroup(rowDef);
                        ArrayList<RowData> rowData = dbGen.getRows();

                        int mapSize = rowDef.getFieldCount() / 8;
                        if (rowDef.getFieldCount() % 8 != 0) {
                            mapSize++;
                        }

                        byte[] columnBitMap = new byte[mapSize];
                        BitSet projection = new BitSet(mapSize);
                        boolean none = true;
                        for (int j = 0; j < rowDef.getFieldCount(); j++) {
                            if (r.nextBoolean()
                               || (none == true && j + 1 == rowDef.getFieldCount())) {
                                projection.set(j, true);
                                columnBitMap[j / 8] |= 1 << (j % 8);
                                none = false;
                            }
                        }
                        DeltaMonitor dm = new DeltaMonitor();
                        
                        VCollector vc = new VCollector(dbGen.getMeta(), dm,
                                rowDefCache, rowDef.getRowDefId(), dbGen
                                        .getGroupBitMap());
                        ByteBuffer buffer = ByteBuffer.allocate(dbGen
                                .getGroupSize());

                        boolean copied = vc.collectNextRow(buffer);
                        buffer.position(0);
                        assertTrue(copied);
                        assertFalse(vc.hasMore());
                        int rowCount = 0;
                        Iterator<RowData> j = rowData.iterator();
                        while (j.hasNext()) {
                            RowData row = j.next();
                            byte[] expected = row.getBytes();
                            byte[] actual = new byte[expected.length];
                            buffer.get(actual);
                            /*
                             * System.out.println(" count = "+rowCount++); int k
                             * = 0; while(k < expected.length) {
                             * System.out.print
                             * (Integer.toHexString(expected[k])+" "); k++; } k
                             * = 0; System.out.println(); while (k <
                             * actual.length) {
                             * System.out.print(Integer.toHexString
                             * (actual[k])+" "); k++; } System.out.println();
                             */
                            assertArrayEquals(expected, actual);
                        }
//                    }
                }
    

            } catch (Exception e) {
                System.out.println("ERROR because " + e.getMessage());
                e.printStackTrace();
                fail("vcollector build failed");
            }
        }
    }

    @Test
    public void testGetProjection() throws Exception {

        /*
         * setupDatabase(); Random rand = new Random(31337); List<RowDef>
         * rowDefs = rowDefCache.getRowDefs(); Iterator<RowDef> i =
         * rowDefs.iterator();
         * 
         * while (i.hasNext()) { RowDef rowDef = i.next(); if
         * (!rowDef.isGroupTable()) { continue; }
         * 
         * BitSet projection = new BitSet(rowDef.getFieldCount());
         * projection.clear(); byte[] bitMap = setupBitMap(projection, rand,
         * rowDef .getFieldCount());
         * 
         * generateEncodedData(rowDef, projection);
         * 
         * VCollector vc = new VCollector(meta, rowDefCache, rowDef
         * .getRowDefId(), bitMap);
         * 
         * assert vc.getProjection().equals(projection);
         * assertTrue(vc.getProjection().equals(projection)); } }
         * 
         * @Test public void testGetUserTables() throws Exception {
         * 
         * setupDatabase(); Random rand = new Random(31337); List<RowDef>
         * rowDefs = rowDefCache.getRowDefs(); Iterator<RowDef> i =
         * rowDefs.iterator();
         * 
         * while (i.hasNext()) {
         * 
         * RowDef rowDef = i.next(); if (!rowDef.isGroupTable()) { continue; }
         * 
         * BitSet projection = new BitSet(rowDef.getFieldCount());
         * projection.clear(); byte[] bitMap = setupBitMap(projection, rand,
         * rowDef .getFieldCount());
         * 
         * generateEncodedData(rowDef, projection);
         * 
         * VCollector vc = new VCollector(meta, rowDefCache, rowDef
         * .getRowDefId(), bitMap);
         * 
         * ArrayList<RowDef> tables = vc.getUserTables();
         * assertTrue(tables.size() > 0); // XXX - implement me. }
         */
    }

    @Test
    public void testManyColumns() throws Exception {

        /*
         * rowDefCache = new RowDefCache();
         * 
         * AkibaInformationSchema ais = null; try { ais = new
         * DDLSource().buildAIS(MANY_DDL); } catch (Exception e1) {
         * e1.printStackTrace(); fail("ais gen failed"); return; }
         * rowDefCache.setAIS(ais); try { List<RowDef> rowDefs =
         * rowDefCache.getRowDefs(); Iterator<RowDef> i = rowDefs.iterator();
         * while (i.hasNext()) {
         * 
         * RowDef rowDef = i.next(); if (!rowDef.isGroupTable()) { continue; }
         * 
         * testRowDef = rowDef;
         * 
         * int mapSize = testRowDef.getFieldCount() / 8; if
         * (testRowDef.getFieldCount() % 8 != 0) { mapSize++; }
         * 
         * byte[] columnBitMap = new byte[mapSize]; BitSet projection = new
         * BitSet(mapSize); for (int j = 0; j < testRowDef.getFieldCount(); j++)
         * { columnBitMap[j / 8] |= 1 << (j % 8); projection.set(j, true); }
         * 
         * columnDes = new ArrayList<IColumnDescriptor>(); columns = new
         * ArrayList<ColumnArrayGenerator>(); encodedColumns = new
         * ArrayList<ArrayList<byte[]>>(); rowData = new ArrayList<RowData>();
         * meta = null;
         * 
         * generateEncodedData(testRowDef, projection);
         * 
         * VCollector vc = new VCollector(meta, rowDefCache, testRowDef
         * .getRowDefId(), columnBitMap);
         * 
         * ByteBuffer buffer = ByteBuffer.allocate((rowSize +
         * RowData.MINIMUM_RECORD_LENGTH + mapSize) rows);
         * 
         * boolean copied = vc.collectNextRow(buffer); assertTrue(copied);
         * assertFalse(vc.hasMore()); Iterator<RowData> j = rowData.iterator();
         * while (j.hasNext()) { RowData row = j.next(); byte[] expected =
         * row.getBytes(); byte[] actual = new byte[expected.length];
         * buffer.get(actual); assertArrayEquals(expected, actual); } } } catch
         * (Exception e) { System.out.println("ERROR because " +
         * e.getMessage()); e.printStackTrace();
         * fail("vcollector build failed"); }
         */
    }

}
