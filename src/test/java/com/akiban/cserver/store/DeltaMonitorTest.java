/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.akiban.cserver.store.DeltaMonitor.DeltaCursor;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

/**
 * @author percent
 *
 */
public class DeltaMonitorTest {

    volatile int count;
    RowDef parentrdef;
    RowDef childrdef;
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
            dm.inserted(ks0, parentrdef, rdata);
            count ++;
            
        }
    }
    
    @Test
    public void testDeltaMonitorLockBlocks() {
        setupTest();
        VStoreTestStub vstore = new VStoreTestStub();
        vstore.threshold = 1048576;
        vstore.datapath = "";
        DeltaMonitor dm = new DeltaMonitor(vstore);
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
    public void testInsertCursor() throws IOException {
        setupTest();
        VStoreTestStub vstore = new VStoreTestStub();
        vstore.threshold = 1048576;
        vstore.datapath = "";
        DeltaMonitor dm = new DeltaMonitor(vstore);
        dm.inserted(ks11, childrdef, rdata);
        dm.inserted(ks1, parentrdef, rdata);
        dm.inserted(ks01, childrdef, rdata);
        dm.inserted(ks0, parentrdef, rdata);
        ArrayList<Integer> ptable =new ArrayList<Integer>();
        ptable.add(parentrdef.getRowDefId()); 
        DeltaCursor cursor = dm.createInsertCursor(ptable);
        Delta d = cursor.get();
        assertEquals(ks0, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(parentrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        
        assertEquals(true, cursor.check(ks1));
        assertEquals(true, cursor.check(ks11));
        assertEquals(true, cursor.check(ks01));
        assertEquals(d, cursor.remove());
        
        d = cursor.get();
        assertEquals(ks1, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(parentrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(false, cursor.check(ks0));
        assertEquals(true, cursor.check(ks11));
        assertEquals(false, cursor.check(ks01));
        assertEquals(d, cursor.remove());
        
        ptable =new ArrayList<Integer>();
        ptable.add(childrdef.getRowDefId()); 
        cursor = dm.createInsertCursor(ptable);
        
        d = cursor.get();
        assertEquals(ks01, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(childrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(true, cursor.check(ks1));
        assertEquals(true, cursor.check(ks11));
        assertEquals(false, cursor.check(ks0));
        assertEquals(d, cursor.remove());
        
        d = cursor.get();
        assertEquals(ks11, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(childrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(false, cursor.check(ks0));
        assertEquals(false, cursor.check(ks01));
        assertEquals(false, cursor.check(ks1));
        assertEquals(d, cursor.remove());
        boolean asserted = false;
        try{
            cursor.remove();
        } catch(AssertionError e) {
            asserted = true;
        }
        assertEquals(true, asserted);
        
        
        ptable =new ArrayList<Integer>();
        ptable.add(parentrdef.getRowDefId());
        ptable.add(childrdef.getRowDefId());
        cursor = dm.createInsertCursor(ptable);
        d = cursor.get();
        assertEquals(ks0, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(parentrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(true, cursor.check(ks1));
        assertEquals(true, cursor.check(ks11));
        assertEquals(true, cursor.check(ks01));
        assertEquals(d, cursor.remove());
        
        d = cursor.get();
        assertEquals(ks01, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(childrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(true, cursor.check(ks1));
        assertEquals(true, cursor.check(ks11));
        assertEquals(false, cursor.check(ks0));
        assertEquals(d, cursor.remove());

        d = cursor.get();
        assertEquals(ks1, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(parentrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(false, cursor.check(ks0));
        assertEquals(true, cursor.check(ks11));
        assertEquals(false, cursor.check(ks01));
        assertEquals(d, cursor.remove());
     
        d = cursor.get();
        assertEquals(ks11, d.getKey());
        assertEquals(rdata, d.getRowData());
        assertEquals(childrdef, d.getRowDef());
        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(false, cursor.check(ks0));
        assertEquals(false, cursor.check(ks01));
        assertEquals(false, cursor.check(ks1));
        assertEquals(d, cursor.remove());
        asserted = false;
        try{
            cursor.remove();
        } catch(AssertionError e) {
            asserted = true;
        }
        assertEquals(true, asserted);

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
                
        parentrdef = rowDefCache.getRowDef(new String("toy_test.parent"));
        assert parentrdef != null;
        
        childrdef = rowDefCache.getRowDef(new String("toy_test.child"));
        assert childrdef != null;

        rdata = new RowData(new byte[420]);
        
        Integer[] values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = new Integer(11);
        rdata.createRow(parentrdef, values);

        //d0 = new Delta(Delta.Type.Insert,ks0, parentrdef, rdata);
        //d01 = new Delta(Delta.Type.Insert,ks01, childrdef, rdata);
        //d1 = new Delta(Delta.Type.Insert,ks1, parentrdef, rdata);
        //d11 = new Delta(Delta.Type.Insert,ks11, childrdef, rdata);
    }
}
