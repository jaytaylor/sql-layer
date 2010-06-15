/**
 * 
 */
package com.akiban.cserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.BitSet;

import org.junit.Test;

import com.akiban.ais.ddl.DDLSource;
import com.akiban.ais.model.AkibaInformationSchema;

/**
 * @author percent
 * 
 */
public class RowDataTest {
    
    RowDefCache rowDefCache;
    AkibaInformationSchema ais;
    RowDef rdef;
    RowData rdata;
    RowData rdata1;
    RowData rdata2;
    RowData rdata3;
    RowData rdataEmpty;
    String VCOLLECTOR_DDL = "src/test/resources/vcollector_test-1.ddl";
    
    public void clearbytes(byte[] store) {
        for (int i = 0; i < store.length; i++) {
            store[i] = 0;
        }
    }

    public boolean checkbytesnotequal(byte[] s0, byte[] s1) {
        boolean ret = false;

        for (int i = 0; i < s0.length; i++) {
            if (s0[i] != s1[1])
                ret = true;
        }
        return ret;
    }

    public void setup() throws Exception {
        rowDefCache = new RowDefCache();

        AkibaInformationSchema ais = null;
        try {
            ais = new DDLSource().buildAIS(VCOLLECTOR_DDL);
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new Exception("ais gen failed");
            
        }

        rowDefCache.setAIS(ais);
    }

    @Test
    public void testCopy() throws Exception {
        setup();
        byte[] s0 = new byte[420];
        byte[] s1 = new byte[420];

        clearbytes(s0);
        clearbytes(s1);

        rdef = rowDefCache.getRowDef(new String("toy_test.parent"));
        assert rdef != null;

        rdata = new RowData(s0);
        rdata1 = new RowData(s1);

        Integer[] values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = new Integer(11);
        assertArrayEquals(rdata.getBytes(), rdata1.getBytes());
        rdata.createRow(rdef, values);
        checkbytesnotequal(s0, s1);
        BitSet nullmap = new BitSet(rdef.getFieldCount());
        rdata1.copy(rdef, rdata, nullmap, 0);
        assertArrayEquals(rdata.getBytes(), rdata1.getBytes());

        nullmap.set(1, true);
        clearbytes(s0);
        clearbytes(s1);
        values[1] = null;
        assertArrayEquals(rdata.getBytes(), rdata1.getBytes());
        rdata.createRow(rdef, values);
        checkbytesnotequal(s0, s1);
        rdata1.copy(rdef, rdata, nullmap, 0);
        assertArrayEquals(rdata.getBytes(), rdata1.getBytes());
    }

    @Test
    public void testIsNull() throws Exception {
        setup();
        byte[] s0 = new byte[420];
        byte[] s1 = new byte[420];

        clearbytes(s0);
        clearbytes(s1);

        rdef = rowDefCache.getRowDef(new String("toy_test.parent"));
        assert rdef != null;

        rdata = new RowData(s0);

        Integer[] values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = new Integer(11);
        rdata.createRow(rdef, values);
        
        assertFalse(rdata.isNull(0));
        assertFalse(rdata.isNull(1));
        
        values[1] = null;
        rdata.createRow(rdef, values);
        assertFalse(rdata.isNull(0));
        assertTrue(rdata.isNull(1));
        
        values[0] = null;
        rdata.createRow(rdef, values);
        assertTrue(rdata.isNull(0));
        assertTrue(rdata.isNull(1));
        
        values[1] = new Integer(420);
        rdata.createRow(rdef, values);
        assertTrue(rdata.isNull(0));
        assertFalse(rdata.isNull(1));
    }

    @Test
    public void testMergeFields() {
        // implement me.
        // fail("Not yet implemented");
    }

}
