/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.BitSet;

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
public class DeltaTest {

    /**
     * Test method for
     * {@link com.akiban.cserver.store.Delta#Delta(com.akiban.cserver.store.Delta.Type, com.persistit.KeyState, com.akiban.cserver.RowDef, com.akiban.cserver.RowData)}
     */
    @Test
    public void testDelta() {

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

        Key key = new Key((Persistit) null);
        key.clear();
        key.append(0);
        KeyState ks0 = new KeyState(key);

        Key key1 = new Key((Persistit) null);
        key1.clear();
        key1.append(1);
        KeyState ks1 = new KeyState(key1);

        key.append(1);
        KeyState ks01 = new KeyState(key);

        key1.append(1);
        KeyState ks11 = new KeyState(key1);

        RowDef rdef = rowDefCache.getRowDef(new String(
                "toy_test.great_grandparent"));
        assert rdef != null;
        RowData rdata = new RowData(new byte[420]);

        Integer[] values = new Integer[1];
        values[0] = new Integer(42);
        rdata.createRow(rdef, values);

        Delta d = new Delta(Delta.Type.Insert, ks1, rdef, rdata);

        assertEquals(Delta.Type.Insert, d.getType());
        assertEquals(rdata, d.getRowData());
        assertEquals(rdef, d.getRowDef());
        assertEquals(ks1, d.getKey());
        assertEquals(false, d.isProjection());

        Delta d1 = new Delta(Delta.Type.Insert, ks0, rdef, rdata);
        assertTrue(d.compareTo(d1) > 0);
        d1 = new Delta(Delta.Type.Insert, ks01, rdef, rdata);
        assertTrue(d.compareTo(d1) > 0);
        d1 = new Delta(Delta.Type.Insert, ks1, rdef, rdata);
        assertTrue(d.compareTo(d1) == 0);
        d1 = new Delta(Delta.Type.Insert, ks11, rdef, rdata);
        assertTrue(d.compareTo(d1) < 0);

        rdef = rowDefCache.getRowDef("toy_test.parent");
        rdata = new RowData(new byte[420]);

        values = new Integer[2];
        values[0] = new Integer(42);
        values[1] = null;
        rdata.createRow(rdef, values);
        d = new Delta(Delta.Type.Insert, ks1, rdef, rdata);
        assertEquals(true, d.isProjection());
        BitSet nullMap = d.getNullMap();

        for (int i = 0; i < rdef.getFieldCount(); i++) {
            if (rdata.isNull(i)) {
                assertTrue(nullMap.get(i));
            } else {
                assertFalse(nullMap.get(i));
            }
        }
    }

}
