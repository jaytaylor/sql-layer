/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.rowdata;

import java.util.Arrays;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.AkServerUtil;
import junit.framework.TestCase;

public class RowDefTest extends TestCase {

    private final static boolean VERBOSE = false;

    private final static String[][] TEST_CASES = new String[][] {
            {
                "create table test(",
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s smallint ", n()),
                ");"
            },
            {
                "create table test(",
                String.format("    %s varchar(100), ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s int ", n()),
                ");"
            },
            {
                "create table test(",
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int ", n()),
                ");"
            },
            {
                "create table test(",
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100), ", n()),
                String.format("    %s varchar(100) ", n()),
                ");"
            },
            {
                "create table test(",
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s smallint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s bigint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s smallint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s bigint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s varchar(200), ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s smallint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s bigint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s smallint, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s bigint, ", n()),
                String.format("    %s varchar(200), ", n()),
                String.format("    %s int, ", n()),
                String.format("    %s int ", n()),
                ");"
            }
    };

    private final static Object[][][] DATA_CASES = new Object[][][] {

            new Object[][] { new Object[] { 1, 2, 3 },
                    new Object[] { null, null, null },
                    new Object[] { null, 2, null },
                    new Object[] { 1, null, 3 }, },

            new Object[][] {
                    new Object[] { null, 2, null, 4, null, 6, null, 8 },
                    new Object[] { null, null, null, null, null, null, null,
                            null },
                    new Object[] { null, 3, "def", 5, "ghi", 7, null, 8 },
                    new Object[] { "a", 2, "b", 4, "c", 6, "d", 8 }, },

            new Object[][] {
                    new Object[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13,
                            14, 15 },
                    new Object[] { null, null, null, null, null, null, null, 8,
                            null, null, null, null, 13, null, 15 }, },

            new Object[][] {
                    new Object[] { "a", "b", "c", "d", "e", "f", "g", "h", "i",
                            "j", "k", "l" },
                    new Object[] { "a", null, "c", null, "e", null, "g", null,
                            "i", null, "k", null },
                    new Object[] { null, "b", null, "d", null, "f", null, null,
                            null, "j", null, "l" },
                    new Object[] { null, null, null, null, null, null, null,
                            null, null, null, null, "end" }, new Object[] {}, },

            new Object[][] { new Object[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
                    11, "foo", 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
                    "bar", 26, 27 }

            }, };

    private static int fieldNameCounter = 0;

    private static String n() {
        return "C" + (++fieldNameCounter);
    }

    public void testComputeFieldLocations1() throws Exception
    {
        SchemaFactory schemaFactory = new SchemaFactory("schema");
        for (int def = 0; def < TEST_CASES.length; def++) {
            String[] ddl = TEST_CASES[def];
            AkibanInformationSchema ais = schemaFactory.aisWithRowDefs(ddl);
            RowDef rowDef = ais.getTable("schema", "test").rowDef();
            FieldDef[] fieldDefs = rowDef.getFieldDefs();
            if (VERBOSE) {
                System.out.println(rowDef);
            }

            for (int data = 0; data < DATA_CASES[def].length; data++) {
                final RowData rowData = new RowData(new byte[1024]);
                rowData.reset(2, 1000); // make sure we can handle non-zero
                // offset
                rowData.createRow(rowDef, DATA_CASES[def][data]);
                if (VERBOSE) {
                    System.out.println("From data: "
                            + Arrays.asList(DATA_CASES[def][data]));
                    System.out.println("Def " + def + " Data " + data);
                    System.out.println("RowData:\n");
                    System.out.println(AkServerUtil.dump(rowData.getBytes(),
                            rowData.getRowStart(), rowData.getRowEnd()
                            - rowData.getRowStart()));
                }
                for (int i = 0; i < fieldDefs.length; i++) {
                    final long location = rowDef.fieldLocation(rowData, i);
                    if (VERBOSE) {
                        System.out.println(String.format(
                                "Field# %3d offset %4d width %4d", i,
                                location & 0xFFFFFFFFL, location >>> 32));
                    }
                    final FieldDef fieldDef = fieldDefs[i];
                    Object value = i < DATA_CASES[def][data].length ? DATA_CASES[def][data][i]
                            : null;

                    assertValuesAreEqual(value, fieldDef, rowData, location);

                }
            }
        }
    }

    // public void dontComputeFieldLocations2() throws Exception {
    // final int fieldCount = 37;
    // final Random random = new Random();
    // final byte[] stringBytes = new byte[100000];
    // for (int i = 0; i < stringBytes.length; i++) {
    // stringBytes[i] = (byte) ((i % 26) + 97);
    // }
    // final FieldDef[] fieldDefs = new FieldDef[fieldCount];
    // final Types[] allTypes = Types.values();
    // int maxSize = 20;
    // for (int i = 0; i < fieldDefs.length; i++) {
    // Types type = allTypes[random.nextInt(allTypes.length)];
    // if (type.isFixedWidth()) {
    // fieldDefs[i] = new FieldDef(n(), type);
    // maxSize += type.getMaxWidth();
    // } else {
    // int max = Math.min(1000, type.getMaxWidth()
    // - type.getMinWidth() + 1);
    // int maxWidth = random.nextInt(max) + type.getMinWidth();
    // fieldDefs[i] = new FieldDef(n(), type, maxWidth);
    // maxSize += maxWidth + 3;
    // }
    // }
    // final RowDef rowDef = new RowDef(fieldCount, fieldDefs);
    // if (VERBOSE) {
    // System.out.println(rowDef);
    // }
    //
    // final Object[] values = new Object[fieldDefs.length];
    // final RowData data = new RowData(new byte[maxSize]);
    //
    // for (int i = 0; i < fieldDefs.length; i++) {
    // Object value = null;
    // if (random.nextInt(10) != 0) {
    // switch (fieldDefs[i].getType()) {
    // case U_TINYINT:
    // case TINYINT:
    // value = (byte) random.nextInt();
    // break;
    // case U_SMALLINT:
    // case SMALLINT:
    // value = (short) random.nextInt();
    // break;
    // case U_MEDIUMINT:
    // case MEDIUMINT:
    // value = random.nextInt() & 0xFFFFFF;
    // break;
    // case FLOAT:
    // case U_INT:
    // case INT:
    // value = random.nextInt();
    // break;
    // case DOUBLE:
    // case U_BIGINT:
    // case BIGINT:
    // // TODO - currently we don't handle unsigned longs.
    // value = random.nextLong() & Long.MAX_VALUE;
    // break;
    // case VARCHAR:
    // case BINCHAR:
    // case BINVARCHAR:
    // case CHAR:
    // int size = random.nextInt(fieldDefs[i].getMaxWidth());
    // value = new byte[size];
    // System.arraycopy(stringBytes, random.nextInt(20000), value,
    // 0, size);
    // default:
    // }
    // values[i] = value;
    // }
    // }
    // data.createRow(rowDef, values);
    //
    // if (VERBOSE) {
    // System.out.println(data);
    // }
    // for (int i = fieldDefs.length; --i >= 0;) {
    // final long location = rowDef.fieldLocation(data, i);
    // assertValuesAreEqual(values[i], fieldDefs[i], data, location);
    // }
    //
    // for (int i = 0; i < 100000; i++) {
    // final int field = random.nextInt(fieldDefs.length);
    // final long location = rowDef.fieldLocation(data, field);
    // assertValuesAreEqual(values[field], fieldDefs[field], data,
    // location);
    // }
    //
    // long xor = 0;
    // int count = 0;
    // final long start = System.nanoTime();
    // while (System.nanoTime() - start < 1000000000L) {
    // for (int k = 0; k < 10000; k++) {
    // for (int i = 0; i < fieldCount; i++) {
    // final long location = rowDef.fieldLocation(data, i);
    // // use the result so that HotSpot doesn't optimize away the
    // // call
    // xor ^= location;
    // }
    // count += fieldDefs.length;
    // }
    // }
    // final long elapsed = System.nanoTime() - start;
    // System.out.println(String.format("Average fieldLocation time on table "
    // + "%d columns wide: %dns (xor=%d)", fieldDefs.length, elapsed
    // / count, xor));
    // }

    private void assertValuesAreEqual(final Object value,
            final FieldDef fieldDef, final RowData rowData, final long location) {
        if (value == null) {
            assertEquals(0, location);
        }
        if (location == 0) {
            assertNull(value);
        } else if (fieldDef.isFixedSize()) {
            long decodedValue = AkServerUtil.getSignedIntegerByWidth(rowData
                    .getBytes(), (int) location, (int) (location >>> 32));
            assertEquals(((Number) value).longValue(), decodedValue);
        } else if (value instanceof String) {
            final String decodedString = rowData.getStringValue((int) location,
                    (int) (location >>> 32), fieldDef);
            assertEquals(value, decodedString);
        } else {
            int prefix = fieldDef.getPrefixSize();
            byte[] decodedBytes = new byte[((int) (location >>> 32)) - prefix];
            System.arraycopy(rowData.getBytes(), (int) location + prefix,
                    decodedBytes, 0, decodedBytes.length);
            assertTrue(Arrays.equals((byte[]) value, decodedBytes));
        }
    }
}
