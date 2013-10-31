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

package com.foundationdb.server.test.it.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.sql.Main;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.server.api.DMLFunctions;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.rowdata.SchemaFactory;
import com.foundationdb.util.GrowableByteBuffer;
import com.foundationdb.util.MySqlStatementSplitter;
import org.junit.After;
import org.junit.Before;

import com.foundationdb.ais.model.Table;
import com.foundationdb.server.AkServerUtil;
import com.foundationdb.server.store.RowCollector;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.util.ByteBufferFactory;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public abstract class AbstractScanBase extends ITBase {

    protected final static String DDL_FILE_NAME = "scan_rows_test.ddl";

    protected final static String SCHEMA = "scan_rows_test";

    protected final static boolean VERBOSE = false;
    
    protected static SortedMap<TableName, Table> tableMap = new TreeMap<>();

    protected List<RowData> result = new ArrayList<>();

    @Before
    public void baseSetUpSuite() throws Exception {
        Set<TableName> created = loadDDLFromResource(SCHEMA, DDL_FILE_NAME);

        final AkibanInformationSchema ais = ddl().getAIS(session());
        for (Table table : ais.getTables().values()) {
            if (table.getName().getTableName().startsWith("a") && created.contains(table.getName())) {
                tableMap.put(new TableName(table.getName().getSchemaName(), table.getName().getTableName()), table);
            }
        }
        
        populateTables(dml());
    }

    @After
    public void baseTearDownSuite() throws Exception {
        tableMap.clear();
    }

    protected Set<TableName> loadDDLFromResource(final String schema, final String resourceName) throws Exception {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                Main.class.getClassLoader().getResourceAsStream(resourceName)));

        List<String> allStatements = new ArrayList<>();

        DDLFunctions ddl = ddl();
        for (String statement : new MySqlStatementSplitter(reader)) {
            if (statement.startsWith("create")) {
                allStatements.add(statement);
            }
        }

        Set<TableName> before = new HashSet<>(ddl.getAIS(session()).getTables().keySet());

        SchemaFactory schemaFactory = new SchemaFactory(schema);
        schemaFactory.ddl(ddl, session(),
                          allStatements.toArray(new String[allStatements.size()]));

        Set<TableName> after = new HashSet<>(ddl.getAIS(session()).getTables().keySet());
        after.removeAll(before);
        return after;
    }

    protected void populateTables(DMLFunctions dml) throws Exception {
        final RowData rowData = new RowData(new byte[256]);
        // Create the tables in alphabetical order. Because of the
        // way the tables are defined, this also creates all parents before
        // their children.
        // PrintStream output = new PrintStream(new FileOutputStream(new File("/tmp/srt.out")));
        for (TableName name : tableMap.keySet()) {
            final RowDef rowDef = getRowDef(name);
            final LegacyRowWrapper rowWrapper = new LegacyRowWrapper(rowDef);
            final int level = name.getTableName().length();
            int k = (int) Math.pow(10, level);
            for (int i = 0; i < k; i++) {
                rowData.createRow(rowDef, new Object[] { (i / 10), i, 7, 8, i + "X" });
                rowWrapper.setRowData(rowData);
                // output.println(rowData.toString(rowDef));
                dml.writeRow(session(), rowWrapper);
            }
        }
        // output.close();
    }

    protected RowDef rowDef(final String name) {
        return getRowDef(new TableName(SCHEMA, name));
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
        final RowCollector rc = store().newRowCollector(session(), scanFlags, rowDefId, indexId, columnBitMap,
                                                        start, null, end, null, null);
        if (VERBOSE) {
            System.out.println("Test " + test);
        }
        rc.open();
        while (rc.hasMore()) {
            final GrowableByteBuffer payload = ByteBufferFactory.allocate(65536);
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
                                       + rowData.toString(ais()));
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

    protected int findIndexId(final RowDef rowDef, final String name) {
        for (Index index : rowDef.getIndexes()) {
            if (index.getIndexName().getName().startsWith(name)) {
                return index.getIndexId();
            }
        }
        return -1;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (final RowData rowData : result) {
            sb.append(rowData.toString(ais()));
            sb.append(AkServerUtil.NEW_LINE);
        }
        return sb.toString();
    }
}
