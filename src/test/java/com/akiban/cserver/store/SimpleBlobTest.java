package com.akiban.cserver.store;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerConstants;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;

public class SimpleBlobTest extends TestCase implements CServerConstants {

    private final static String CREATE_TABLE_STATEMENT1 = "CREATE TABLE `test`.`blobtest` ("
            + "`a` int,"
            + "`b` blob,"
            + "`c` blob,"
            + "PRIMARY KEY (a)"
            + ") ENGINE=AKIBADB;";

    private PersistitStore store;

    private RowDefCache rowDefCache;

    @Override
    public void setUp() throws Exception {
        rowDefCache = new RowDefCache();
        store = new PersistitStore(CServerConfig.unitTestConfig(), rowDefCache);
        final AkibaInformationSchema ais = new DDLSource().buildAISFromString(CREATE_TABLE_STATEMENT1);
        rowDefCache.setAIS(ais);
        store.startUp();
    }

    @Override
    public void tearDown() throws Exception {
        store.shutDown();
        store = null;
        rowDefCache = null;
    }
    
    public void testBlobs() throws Exception {
        final RowDef rowDef = rowDefCache.getRowDef("test.blobtest");
        final RowData rowData =new RowData(new byte[5000000]);
        final String[] expected = new String[7];
        for (int i = 1; i <= 6; i++) {
            int bsize = (int)Math.pow(5, i);
            int csize = (int)Math.pow(10, i);
            rowData.createRow(rowDef, new Object[]{i, bigString(bsize), bigString(csize)});
            expected[i] = rowData.toString(rowDefCache);
            assertEquals( OK, store.writeRow(rowData));
        }
        
        final RowCollector rc = store.newRowCollector(rowDef.getRowDefId(), 0, 0, null, null, new byte[]{7});
        final ByteBuffer bb = ByteBuffer.allocate(5000000);
        for (int i = 1; i <= 6; i++) {
            assertTrue(rc.hasMore());
            bb.clear();
            assertTrue(rc.collectNextRow(bb));
            bb.flip();
            rowData.reset(bb.array(), 0, bb.limit());
            rowData.prepareRow(0);
            final String actual = rowData.toString(rowDefCache);
            assertEquals(expected[i], actual);
        }
     }

    private String bigString(final int length) {
        final StringBuilder sb= new StringBuilder(length);
        sb.append(length);
        for (int i = sb.length() ; i < length; i++) {
            sb.append("#");
        }
        return sb.toString();
    }
}
