package com.akiban.cserver;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.store.PersistitStore;

/**
 * A preliminary unit test to verify HKey definition field in RowDefCache are
 * set up as specified in the schema.
 * 
 * @author peter
 * 
 */
public class RowDefCacheTest {

    protected final static String DDL_FILE_NAME = "src/test/resources/row_def_cache_test.ddl";

    protected final static String SCHEMA = "row_def_cache_test";

    protected static PersistitStore store;

    protected static RowDefCache rowDefCache;

    @BeforeClass
    public static void setUpSuite() throws Exception {

        rowDefCache = new RowDefCache();
        store = new PersistitStore(CServerConfig.unitTestConfig(), rowDefCache);
    }

    @AfterClass
    public static void tearDownSuite() throws Exception {
        store.shutDown();
        store = null;
        rowDefCache = null;
    }

    @Test
    public void testHKeyOrdering() throws Exception {
        final AkibaInformationSchema ais = new DDLSource()
                .buildAIS(DDL_FILE_NAME);
        rowDefCache.setAIS(ais);
        final RowDef b = rowDefCache.getRowDef(SCHEMA + ".b");
        final RowDef bb = rowDefCache.getRowDef(SCHEMA + ".bb");

        assertTrue(Arrays.equals(new int[] { 3, 2, 4, 1 }, b.getPkFields()));
        assertTrue(Arrays.equals(new int[] {}, b.getParentJoinFields()));
        assertTrue(Arrays.equals(new int[] { 5, 4 }, bb.getPkFields()));
        assertTrue(Arrays.equals(new int[] { 0, 2, 1, 3 }, bb
                .getParentJoinFields()));
    }
}
