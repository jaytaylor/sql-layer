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

package com.akiban.server.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.server.AkServerTestSuite;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.akiban.ais.model.UserTable;
import com.akiban.server.AkServerUtil;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.util.ByteBufferFactory;

public abstract class AbstractScanBase extends AkServerTestSuite {

    protected final static String DDL_FILE_NAME = "scan_rows_test.ddl";

    protected final static String SCHEMA = "scan_rows_test";


    protected final static boolean VERBOSE = true;
    

    protected static SortedMap<String, UserTable> tableMap = new TreeMap<String, UserTable>();

    protected List<RowData> result = new ArrayList<RowData>();

    @BeforeClass
    public static void baseSetUpSuite() throws Exception {
        AkServerTestSuite.setUpSuite();
        
        //rowDefCache.setAIS(ais0);
        final AkibanInformationSchema ais = setUpAisForTests(DDL_FILE_NAME);
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getName().getTableName().startsWith("a")) {
                tableMap.put(table.getName().getSchemaName() + "."
                        + table.getName().getTableName(), table);
            }
        }
        populateTables();
    }

    @AfterClass
    public static void baseTearDownSuite() throws Exception {
        AkServerTestSuite.tearDownSuite();
        tableMap.clear();
    }

    protected static void populateTables() throws Exception {
        final RowData rowData = new RowData(new byte[256]);
        // Create the tables in alphabetical order. Because of the
        // way the tables are defined, this also creates all parents before
        // their children.
        // PrintStream output = new PrintStream(new FileOutputStream(new File("/tmp/srt.out")));
        for (String name : tableMap.keySet()) {
            final RowDef rowDef = rowDefCache.getRowDef(name);
            final int level = name.length() - SCHEMA.length() - 1;
            int k = (int) Math.pow(10, level);
            for (int i = 0; i < k; i++) {
                rowData.createRow(rowDef, new Object[] { (i / 10), i, 7, 8,
                        i + "X" });
                // output.println(rowData.toString(rowDefCache));
                try {
                    store.writeRow(session, rowData);
                }
                catch (Throwable t) {
                    throw new Exception(t);
                }
            }
        }
        // output.close();
    }

    protected RowDef rowDef(final String name) {
        return rowDefCache.getRowDef(SCHEMA + "." + name);
    }

    protected int scanAllRows(final String test, final RowData start,
            final RowData end, final byte[] columnBitMap, final int indexId)
            throws Exception {
        return scanAllRows(test, start.getRowDefId(), 0, start, end,
                columnBitMap, indexId);
    }

    protected int scanAllRows(final String test, final int rowDefId,
            final int scanFlags, final RowData start, final RowData end,
            final byte[] columnBitMap, final int indexId) throws Exception {
        int scanCount = 0;
        result.clear();
        final RowCollector rc = store.newRowCollector(session, rowDefId, indexId,
                scanFlags, start, end, columnBitMap, null);
        if (VERBOSE) {
            System.out.println("Test " + test);
        }
        while (rc.hasMore()) {
            final ByteBuffer payload = ByteBufferFactory.allocate(65536);
            while (rc.collectNextRow(payload))
                ;
            payload.flip();
            for (int p = payload.position(); p < payload.limit();) {
                RowData rowData = new RowData(payload.array(),
                        payload.position(), payload.limit());
                rowData.prepareRow(p);
                scanCount++;
                result.add(rowData);
                if (VERBOSE) {
                    System.out.println(String.format("%5d ", scanCount)
                            + rowData.toString(rowDefCache));
                }
                p = rowData.getRowEnd();
            }
        }
        rc.close();
        if (VERBOSE) {
            System.out.println();
        }
        return scanCount;
    }

    protected int findIndexId(final RowDef groupRowDef,
            final RowDef userRowDef, final int fieldIndex) {
        final int findField = fieldIndex + userRowDef.getColumnOffset()
                - groupRowDef.getColumnOffset();
        for (final IndexDef indexDef : groupRowDef.getIndexDefs()) {
            if (indexDef.getFields().length == 1
                    && indexDef.getFields()[0] == findField) {
                return indexDef.getId();
            }
        }
        return -1;
    }

    protected int findIndexId(final RowDef rowDef, final String name) {
        for (final IndexDef indexDef : rowDef.getIndexDefs()) {
            if (indexDef.getName().startsWith(name)) {
                return indexDef.getId();
            }
        }
        return -1;
    }

    protected void assertOk(final int a, final int b) {
        System.out.println("AssertOk expected and got " + a + "," + b);
    }

    protected int findFieldIndex(final RowDef rowDef, final String name) {
        for (int index = 0; index < rowDef.getFieldCount(); index++) {
            if (rowDef.getFieldDef(index).getName().equals(name)) {
                return index;
            }
        }
        return -1;
    }

    protected int columnOffset(final RowDef userRowDef, final RowDef groupRowDef) {
        for (int i = groupRowDef.getUserTableRowDefs().length; --i >= 0;) {
            if (groupRowDef.getUserTableRowDefs()[i] == userRowDef) {
                return groupRowDef.getUserTableRowDefs()[i].getColumnOffset();
            }
        }
        return -1;
    }

    protected byte[] bitsToRoot(final RowDef rowDef, final RowDef groupRowDef) {
        final byte[] bits = new byte[(groupRowDef.getFieldCount() + 7) / 8];
        for (RowDef rd = rowDef; rd != null;) {
            int level = -1;
            for (int i = 0; i < groupRowDef.getUserTableRowDefs().length; i++) {
                if (groupRowDef.getUserTableRowDefs()[i] == rd) {
                    level = i;
                    break;
                }
            }
            // Set the bits for all columns of the user table selected by level
            RowDef userTableRowDef = groupRowDef.getUserTableRowDefs()[level];
            UserTable userTable = userTableRowDef.userTable();
            for (Column column : userTable.getColumns()) {
                int groupColumnPosition = userTableRowDef.getColumnOffset() + column.getPosition();
                bits[groupColumnPosition / 8] |= 1 << (groupColumnPosition % 8);
            }
            if (rd.getParentRowDefId() == 0) {
                break;
            } else {
                rd = rowDefCache.getRowDef(rd.getParentRowDefId());
            }
        }
        return bits;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final RowData rowData : result) {
            sb.append(rowData.toString(rowDefCache));
            sb.append(AkServerUtil.NEW_LINE);
        }
        return sb.toString();
    }
}
