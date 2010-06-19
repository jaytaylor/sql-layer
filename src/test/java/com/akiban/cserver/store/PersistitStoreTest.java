package com.akiban.cserver.store;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;
import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.CServerConfig;
import com.akiban.cserver.CServerUtil;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.decider.DecisionEngine;

public class PersistitStoreTest {

    private final static File DATA_PATH = new File("/tmp/data");
    private final static String DDL_FILE_NAME = "src/test/resources/vcollector_test-1.ddl";
    private PersistitStore store;
    private RowDefCache rowDefCache;
    private AkibaInformationSchema ais;

    private void loadVData() throws Exception {
        List<RowDef> rowDefs = rowDefCache.getRowDefs();
        Iterator<RowDef> i = rowDefs.iterator();
        while (i.hasNext()) {
            RowDef rowDef = i.next();
            if (!rowDef.isGroupTable()) {
                continue;
            }
            VStoreTestStub vstore = new VStoreTestStub();
            vstore.threshold = 1048576;
            vstore.datapath = "";
            
            GroupGenerator dbGen = new GroupGenerator(
                    DATA_PATH.getAbsolutePath()+"/vstore/", ais, rowDefCache,
                    vstore, false, true);
            dbGen.generateGroup(rowDef, 2);
            dbGen.getMeta().write(new File(DATA_PATH.getAbsoluteFile()+"/vstore/.vmeta"));
        }
    }

    @Test
    public void testConfig() throws Exception {

        CServerConfig config = CServerConfig.unitTestConfig();

        // config.setProperty("cserver.decision_engine", "vstore");
        rowDefCache = new RowDefCache();
        try {
            store = new PersistitStore(config, rowDefCache);
        } catch (Exception e) {
            e.printStackTrace();
            fail("failed to create Persistit");
        }

        assertEquals(DecisionEngine.Strategy.HStore, store.getDecisionEngine()
                .getStrategy());
        assertEquals(false, store.isDeltaMonitorActivated());
        assertEquals(1048576, store.getDeltaThreshold());
        assertEquals(null, store.getVMeta());

        // config.setProperty("cserver.decision_engine", "vstore");
        config.setProperty("cserver.delta_threshold", "10");
        config.setProperty("cserver.delta_store", "on");

        rowDefCache = new RowDefCache();
        try {
            store = new PersistitStore(config, rowDefCache);
        } catch (Exception e) {
            e.printStackTrace();
            fail("failed to create Persistit");
        }

        assertEquals(DecisionEngine.Strategy.HStore, store.getDecisionEngine()
                .getStrategy());
        assertEquals(true, store.isDeltaMonitorActivated());
        assertEquals(10, store.getDeltaThreshold());
        assertEquals(null, store.getVMeta());

        config.setProperty("cserver.decision_engine", "vstore");
        config.setProperty("cserver.delta_threshold", "1000");
        config.setProperty("cserver.delta_store", "off");

        try {
            store = new PersistitStore(config, rowDefCache);
        } catch (Exception e) {
            e.printStackTrace();
            fail("failed to create Persistit");
        }

        assertEquals(DecisionEngine.Strategy.VStore, store.getDecisionEngine()
                .getStrategy());
        assertEquals(false, store.isDeltaMonitorActivated());
        assertEquals(1000, store.getDeltaThreshold());
        assertEquals(null, store.getVMeta());

        CServerUtil.cleanUpDirectory(DATA_PATH);
        PersistitStore.setDataPath(DATA_PATH.getPath());
        
        CServerUtil.cleanUpDirectory(DATA_PATH);
        PersistitStore.setDataPath(DATA_PATH.getPath());
        ais = new DDLSource().buildAIS(DDL_FILE_NAME);
        rowDefCache.setAIS(ais);

        loadVData();
      
        store.startUp();
      
        assertTrue((null != store.getVMeta()));
        
        store.setOrdinals();
        store.shutDown();
        store = null;
        rowDefCache = null;
    }

}
