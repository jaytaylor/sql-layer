/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.test.it.rowtests;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.test.it.ITBase;
import junit.framework.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UnsignedFieldsIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";
    private final boolean IS_PK = false;
    private final boolean INDEXES = false;
    
    private void writeRows(int tableId, Object... values) {
        long id = 0;
        for(Object o : values) {
            dml().writeRow(session(),  createNewRow(tableId, id++, o));
        }
    }

    private void compareRows(int tableId, Object... values) {
        List<NewRow> rows = scanAll(scanAllRequest(tableId));
        assertEquals("column count", 2, getUserTable(tableId).getColumns().size());
        Iterator<NewRow> rowIt = rows.iterator();
        Iterator<Object> expectedIt = Arrays.asList(values).iterator();
        while(rowIt.hasNext() && expectedIt.hasNext()) {
            NewRow row = rowIt.next();
            Object actual = row.get(1);
            Object expected = expectedIt.next();
            assertEquals("row id " + row.get(0), expected, actual);
        }
        String extra = "";
        while(rowIt.hasNext()) {
            extra += rowIt.next() + ",";
        }
        if(!extra.isEmpty()) {
            Assert.fail("Extra rows from scan: " + extra);
        }
        while(expectedIt.hasNext()) {
            extra += expectedIt.next() + ",";
        }
        if(!extra.isEmpty()) {
            Assert.fail("Expected more rows from scan: " + extra);
        }
    }

    private void writeRowsAndCompareValues(int tableId, Object... values) {
        writeRows(tableId,  values);
        compareRows(tableId, values);
    }

    private Object[] getTestValues(int bitCount) {
        long signedMax = (1L << (bitCount - 1)) - 1;
        return array(0L, 1L, signedMax - 2, signedMax - 1, signedMax, signedMax + 1, signedMax + 2, signedMax*2 + 1);
    }

    
    @Test
    public void tinyIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "tinyint unsigned");
        Object[] values = getTestValues(8);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void smallIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "smallint unsigned");
        Object[] values = getTestValues(16);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void mediumIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "mediumint unsigned");
        Object[] values = getTestValues(24);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void intUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "int unsigned");
        Object[] values = getTestValues(32);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void bigIntUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "bigint unsigned");
        Object[] values = {new BigInteger("0"), new BigInteger("1"),
                           new BigInteger("9223372036854775805"), new BigInteger("9223372036854775806"),
                           new BigInteger("9223372036854775807"),
                           new BigInteger("9223372036854775808"), new BigInteger("9223372036854775809"),
                           new BigInteger("18446744073709551615")};
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal52Unsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c1", "decimal unsigned", 5L, 2L));
        Object[] values = array(new BigDecimal("0.00"), new BigDecimal("1.00"),
                                new BigDecimal("499.99"), new BigDecimal("500.00"), new BigDecimal("999.99"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal2010Unsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, new SimpleColumn("c1", "decimal unsigned", 20L, 10L));
        Object[] values = array(new BigDecimal("0.0000000000"), new BigDecimal("1.0000000000"),
                                new BigDecimal("4999999999.9999999999"), new BigDecimal("5000000000.0000000000"),
                                new BigDecimal("9999999999.9999999999"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void floatUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "float unsigned");
        Object[] values = array(0.0f, 1.0f, Float.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
    
    @Test
    public void doubleUnsigned() {
        int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "double unsigned");
        Object[] values = array(0.0d, 1.0d, Double.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
}
