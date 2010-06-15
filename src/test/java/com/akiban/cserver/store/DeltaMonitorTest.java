/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

/**
 * @author percent
 *
 */
public class DeltaMonitorTest {

    volatile int count;
    RowDef rdef;
    RowData rdata;
    Key key;
    Key key1;
    KeyState ks0;
    KeyState ks01;
    KeyState ks1;
    KeyState ks11;
    Delta d0;
    Delta d01;
    Delta d1;
    Delta d11;

    public class WriteLockTester extends Thread {
        DeltaMonitor dm;
        public WriteLockTester(DeltaMonitor dm) {
            count = 0;
            this.dm = dm;
        }
        public void run() {
            count ++;
            dm.inserted(ks0, rdef, rdata);
            count ++;
            
        }
    }
    
    @Test
    public void testDeltaMonitorLockBlocks() {
        setupTest();
        DeltaMonitor dm = new DeltaMonitor();
        dm.readLock();
        WriteLockTester locktester = new WriteLockTester(dm);
        assertEquals(0, count);
        locktester.start();
        while(count == 0) ;
        assertEquals(1, count);
        dm.releaseReadLock();
        while(count == 1) ;
        assertEquals(2, count);
    }
    
    @Test
    public void testMergeInsert() throws IOException {
        setupTest();
        DeltaMonitor dm = new DeltaMonitor();
        dm.inserted(ks0, rdef, rdata);
        dm.inserted(ks1, rdef, rdata);
        dm.configureInsertCursor(rdef.getRowDefId());
        BitSet nullmap = new BitSet(rdef.getFieldCount());
        for(int i = 0; i < rdef.getFieldCount(); i++) {
            nullmap.set(i, true);
        }
        boolean copied = dm.mergeInsert(ks01, rdef.getRowDefId(), rdata, nullmap, 0);
        assertTrue(copied);
        boolean assertionTriggered = false;
        try {
        copied = dm.mergeInsert(ks1, rdef.getRowDefId(), rdata, nullmap, 0);
        } catch(AssertionError e) {
            assertionTriggered = true;
        }
        assertTrue(assertionTriggered);
        
        copied = dm.mergeInsert(ks11, rdef.getRowDefId(), rdata, nullmap, 0);
        assertTrue(copied);
        
        copied = dm.mergeInsert(ks11, rdef.getRowDefId(), rdata, nullmap, 0);
        assertFalse(copied);
    }
    
    public void setupTest() {
       String VCOLLECTOR_DDL = "src/test/resources/vcollector_test-1.ddl";
        
        RowDefCache rowDefCache = new RowDefCache();

        AkibaInformationSchema ais = null;
        try {
            ais = new DDLSource().buildAIS(VCOLLECTOR_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            fail("ais gen failed");
            return;
        }
        
        rowDefCache.setAIS(ais);
        
        key = new Key((Persistit)null);
        key.clear();
        key.append(0);
        ks0 = new KeyState(key);

        key1 = new Key((Persistit)null);
        key1.clear();
        key1.append(1);
        ks1 = new KeyState(key1);
        
        key.append(1);
        ks01 = new KeyState(key);
        
        key1.append(1);
        ks11 = new KeyState(key1);
                
        rdef = rowDefCache.getRowDef(new String("toy_test.parent"));
        assert rdef != null;
        rdata = new RowData(new byte[420]);
        
        Integer[] values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = new Integer(11);
        rdata.createRow(rdef, values);
        

        d0 = new Delta(Delta.Type.Insert,ks0, rdef, rdata);
        d01 = new Delta(Delta.Type.Insert,ks01, rdef, rdata);
        d1 = new Delta(Delta.Type.Insert,ks1, rdef, rdata);
        d11 = new Delta(Delta.Type.Insert,ks11, rdef, rdata);

        
       rdef = rowDefCache.getRowDef("toy_test.parent");
       rdata = new RowData(new byte[420]);
        
        values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = null;
        rdata.createRow(rdef, values);
        /*d = new Delta(Delta.Type.Insert,ks1, rdef, rdata);
        BitSet nullMap = d.getNullMap();
        
        for(int i = 0; i < rdef.getFieldCount(); i++) {
            if(rdata.isNull(i)) {
                assertTrue(nullMap.get(i));
            } else {
                assertFalse(nullMap.get(i));
            }
        }
        */
    }
}
