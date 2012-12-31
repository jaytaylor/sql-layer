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

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

public class FieldToFromObjectIT extends ITBase {
    private final String SCHEMA = "test";
    private final String TABLE = "t";
    private final boolean IS_PK = false;
    private final boolean INDEXES = false;
    private final RowData rowData = new RowData(new byte[4096]);

    private void testRow(final RowDef rowDef, Object ...values) {
        rowData.reset(0, rowData.getBytes().length);
        try {
            rowData.createRow(rowDef, values);
        }
        catch(Exception e) {
            throw new RuntimeException(String.format("createRow() failed for table %s and values %s",
                    rowDef.table().getName(), Arrays.asList(values)), e);
        }
    }

    @Test
    public void signedIntTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             "tinyint", "smallint", "mediumint", "int", "bigint");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, 0, 0, 0, 0, 0);                                              // zero
        testRow(def, 2, -128, -32768, -8388608, -2147483648, -9223372036854775808L); // min
        testRow(def, 3, 127, 32767, 8388607, 2147483647, 9223372036854775807L);      // max
        testRow(def, 4, 35, 23964, 7904325, -148932453, 86937294587L);               // other
    }

    @Test
    public void unsignedIntTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             "tinyint unsigned", "smallint unsigned", "mediumint unsigned", "int unsigned", "bigint unsigned");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, 0, 0, 0, 0, BigInteger.ZERO);                                               // zero/min
        testRow(def, 2, 255, 65535, 16777215, 4294967295L, new BigInteger("18446744073709551615")); // max
        testRow(def, 3, 42, 9848, 2427090, 290174268L, new BigInteger("73957261119487228"));        // other
    }

    @Test
    public void signedRealTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "float", "double");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, 0f, 0d);                            // zero
        testRow(def, 2, Float.MIN_VALUE, Double.MIN_VALUE); // min
        testRow(def, 3, Float.MAX_VALUE, Double.MAX_VALUE); // max
        testRow(def, 4, -10f, -100d);                       // negative whole
        testRow(def, 5, -123.456f, -8692.18474823d);        // negative fraction
        testRow(def, 6, 432f, 398483298d);                  // positive whole
        testRow(def, 7, 75627.872743f, 99578294.938483d);   // positive fraction
    }

    @Test
    public void unsignedRealTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES, "float unsigned","double unsigned");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, 0f, 0d);                            // zero
        testRow(def, 2, Float.MAX_VALUE, Double.MAX_VALUE); // max
        testRow(def, 3, 12345f, 9876543210d);               // positive whole
        testRow(def, 4, 7234.1321f, 3819476924.12342819d);  // positive fraction
    }

    @Test
    public void decimalTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             new SimpleColumn("c1", "decimal", 5L, 2L), new SimpleColumn("c2", "decimal unsigned", 5L, 2L));
        final RowDef def = getRowDef(tid);
        testRow(def, 1, BigDecimal.valueOf(0), BigDecimal.valueOf(0));                // zero
        testRow(def, 2, BigDecimal.valueOf(-99999L, 2), BigDecimal.valueOf(0));       // min
        testRow(def, 3, BigDecimal.valueOf(99999L, 2), BigDecimal.valueOf(99999, 2)); // max
        testRow(def, 4, BigDecimal.valueOf(-999L), BigDecimal.valueOf(0));            // negative whole
        testRow(def, 5, BigDecimal.valueOf(-12345, 1), BigDecimal.valueOf(0));        // negative fraction
        testRow(def, 6, BigDecimal.valueOf(567L), BigDecimal.valueOf(10));            // positive whole
        testRow(def, 7, BigDecimal.valueOf(425, 1), BigDecimal.valueOf(76543, 2));    // positive fraction
    }

    @Test
    public void charTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             new SimpleColumn("c1", "char", 10L, null), new SimpleColumn("c2", "varchar", 26L, null));
        final RowDef def = getRowDef(tid);
        testRow(def, 1, "", "");                                     // empty
        testRow(def, 2, "0123456789", "abcdefghijklmnopqrstuvwxyz"); // full
        testRow(def, 3, "zebra", "see spot run");                    // other
    }

    @Test
    public void blobTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             "tinyblob", "blob", "mediumblob", "longblob");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, "", "", "", "");            // empty
        testRow(def, 2, "a", "bc", "def", "hijk");  // other
    }

    @Test
    public void textTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             "tinytext", "text", "mediumtext", "longtext");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, "", "", "", "");            // empty
        testRow(def, 2, "1", "23", "456", "7890");  // other
    }

    @Test
    public void binaryTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             new SimpleColumn("c1", "binary", 10L, null), new SimpleColumn("c2", "varbinary", 26L, null));
        final RowDef def = getRowDef(tid);
        final byte[] emptyArr = {};
        final byte[] partialArr5 = {1, 2, 3, 4, 5};
        final byte[] partialArr15 = {-24, 8, -98, 45, 67, 127, 34, -34, -42, 9, 10, 100, 57, -20, 5};
        final byte[] fullArr10 = {0, -10, 24, -128, 127, 83, 97, 100, -120, 42};
        final byte[] fullArr26 = {-47, -48, -66, 98, -54, -12, -53, -100, -25, -80, 9, 29, 17, -39, 45, 44, 15,
                                    8, -35, 119, -83, 32, -119, 17, 126, -112};
        testRow(def, 1, emptyArr, emptyArr);
        testRow(def, 2, fullArr10, fullArr26);
        testRow(def, 3, partialArr5, partialArr15);
    }

    @Test
    public void dateAndTimeTypes() throws InvalidOperationException {
        final int tid = createTableFromTypes(SCHEMA, TABLE, IS_PK, INDEXES,
                                             "date", "time", "datetime", "timestamp", "year");
        final RowDef def = getRowDef(tid);
        testRow(def, 1, "0000-00-00", "00:00:00", "0000-00-00 00:00:00", 0L, "0000");           // zero
        testRow(def, 2, "1000-01-01", "-838:59:59", "1000-01-01 00:00:00", 0L, "1901");         // min
        testRow(def, 3, "9999-12-31", "838:59:59", "9999-12-31 23:59:59", 2147483647L, "2155"); // max
        testRow(def, 4, "2011-05-20", "17:34:20", "2011-05-20 17:35:01", 1305927301L, "2011");  // other
    }
}
