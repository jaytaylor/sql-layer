/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;

/**
 * @author percent
 * 
 */
public class VRewriteHelperTest {

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

    public boolean delete(File path) {
        if (path.exists()) {
            File[] files = path.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    delete(files[i]);
                } else {
                    files[i].delete();
                }
            }
        }
        return (path.delete());
    }

    @After
    public void tearDown() throws Exception {
        File file = new File(VCOLLECTOR_TEST_DATADIR + "/vstore");
        // assert delete(file);
    }

    @Test
    public void testDeltaWriter() throws Exception {

        try {
            // boolean one = false;
            setupDatabase();
            List<RowDef> rowDefs = rowDefCache.getRowDefs();
            Iterator<RowDef> i = rowDefs.iterator();
            while (i.hasNext()) {
                // if (one)
                // return;

                RowDef rowDef = i.next();
                if (!rowDef.isGroupTable()) {
                    continue;
                }
                // one = true;
                File file = new File(VCOLLECTOR_TEST_DATADIR + "/vstore");
                delete(file);

                VStoreTestStub vstore = new VStoreTestStub();
                vstore.threshold = 1048576;
                vstore.datapath = VCOLLECTOR_TEST_DATADIR;
                GroupGenerator dbGen = new GroupGenerator(vstore.datapath, ais,
                        rowDefCache, vstore, false, true);
                dbGen.generateGroup(rowDef);
                ArrayList<RowData> rowData = dbGen.getRows();

                VRewriteHelper vrewriter = new VRewriteHelper(dbGen.getMeta(),
                        dbGen.getDeltas().createInsertCursor(), dbGen
                                .getDeltas().getTables());
                Iterator<RowData> j = rowData.iterator();
                // System.out.println("rowdata.size ()" = )
                assertEquals(rowData.size(), vrewriter.getTotalRows());
                for (int rowCount = 0; rowCount < vrewriter.getTotalRows(); rowCount++) {
                    Delta d = vrewriter.getNextRow();
                    RowData row = j.next();
                    byte[] expected = row.getBytes();
                    byte[] actual = d.getRowData().getBytes();

                    /*
                     * System.out.println(" count = "+rowCount++); int k = 0;
                     * while(k < expected.length) { System.out.print
                     * (Integer.toHexString(expected[k])+" "); k++; } k = 0;
                     * System.out.println(); while (k < actual.length) {
                     * System.out.print(Integer.toHexString (actual[k])+" ");
                     * k++; } System.out.println();
                     */
                    assertArrayEquals(expected, actual);

                }

            }

        } catch (Exception e) {
            System.out.println("ERROR because " + e.getMessage());
            e.printStackTrace();
            fail("vcollector build failed");
        } finally {
            File file = new File(VCOLLECTOR_TEST_DATADIR + "/vstore");
            file.delete();
        }
    }
}
