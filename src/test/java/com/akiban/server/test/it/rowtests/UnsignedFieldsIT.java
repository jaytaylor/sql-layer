/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.test.it.rowtests;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.UnsupportedDataTypeException;
import com.akiban.server.test.it.ITBase;
import junit.framework.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UnsignedFieldsIT extends OldTypeITBase {
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
        int tid = createTableFromTypes("tinyint unsigned");
        Object[] values = getTestValues(8);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void smallIntUnsigned() {
        int tid = createTableFromTypes("smallint unsigned");
        Object[] values = getTestValues(16);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void mediumIntUnsigned() {
        int tid = createTableFromTypes("mediumint unsigned");
        Object[] values = getTestValues(24);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void intUnsigned() {
        int tid = createTableFromTypes("int unsigned");
        Object[] values = getTestValues(32);
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void bigIntUnsigned() {
        int tid = createTableFromTypes("bigint unsigned");
        Object[] values = {new BigInteger("0"), new BigInteger("1"),
                           new BigInteger("9223372036854775805"), new BigInteger("9223372036854775806"),
                           new BigInteger("9223372036854775807"),
                           new BigInteger("9223372036854775808"), new BigInteger("9223372036854775809"),
                           new BigInteger("18446744073709551615")};
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal52Unsigned() {
        int tid = createTableFromTypes(new TypeAndParams("decimal unsigned", 5L, 2L));
        Object[] values = array(new BigDecimal("0.00"), new BigDecimal("1.00"),
                                new BigDecimal("499.99"), new BigDecimal("500.00"), new BigDecimal("999.99"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void decimal2010Unsigned() {
        int tid = createTableFromTypes(new TypeAndParams("decimal unsigned", 20L, 10L));
        Object[] values = array(new BigDecimal("0.0000000000"), new BigDecimal("1.0000000000"),
                                new BigDecimal("4999999999.9999999999"), new BigDecimal("5000000000.0000000000"),
                                new BigDecimal("9999999999.9999999999"));
        writeRowsAndCompareValues(tid, values);
    }

    @Test
    public void floatUnsigned() {
        int tid = createTableFromTypes("float unsigned");
        Object[] values = array(0.0f, 1.0f, Float.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
    
    @Test
    public void doubleUnsigned() {
        int tid = createTableFromTypes("double unsigned");
        Object[] values = array(0.0d, 1.0d, Double.MAX_VALUE);
        writeRowsAndCompareValues(tid, values);
    }
}
