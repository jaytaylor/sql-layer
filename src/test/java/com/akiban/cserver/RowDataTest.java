/**
 * 
 */
package com.akiban.cserver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
    public void testMergeFields() {
        // implement me.
        // fail("Not yet implemented");
    }

}
