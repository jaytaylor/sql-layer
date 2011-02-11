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

import static com.akiban.server.store.RowCollector.SCAN_FLAGS_END_AT_EDGE;
import static com.akiban.server.store.RowCollector.SCAN_FLAGS_START_AT_EDGE;
import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.akiban.server.AkServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.server.AkServerTestCase;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.config.Property;
import com.akiban.util.ByteBufferFactory;
import com.akiban.util.MySqlStatementSplitter;

public class MultiVolumeStoreTest extends AkServerTestCase {

    private final static String DDL_FILE_NAME = "data_dictionary_test.ddl";
    private final static String DEFAULT_SCHEMA = "default_schema";
    private final static String TEST1_SCHEMA = "test1";
    private final static String TEST2_SCHEMA = "test2";

    protected List<RowData> result = new ArrayList<RowData>();

    @Before
    public void setUp() throws Exception {
        // Set up multi-volume treespace policy so we can be sure schema is
        // properly distributed.
        final Collection<Property> properties = new ArrayList<Property>();
        properties.add(property("cserver", "treespace.1",
                "test2/_akiban_customer:${datapath}/${schema}_customer.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        properties.add(property("cserver", "treespace.2",
                "test*:${datapath}/${schema}.v0,create,pageSize:8K,"
                        + "initialSize:10K,extensionSize:1K,maximumSize:10G"));
        baseSetUp(properties);
        //
        // Load the data_dictionary_test tables into three different schemas.
        //
        final BufferedReader reader = new BufferedReader(new InputStreamReader(
                AkServer.class.getClassLoader().getResourceAsStream(
                        DDL_FILE_NAME)));
        for (String statement : (new MySqlStatementSplitter(reader))) {
            if (statement.startsWith("create")) {
                for (final String schemaName : new String[] { DEFAULT_SCHEMA,
                        TEST1_SCHEMA, TEST2_SCHEMA }) {
                    schemaManager.createTableDefinition(session, schemaName,
                            statement, false);
                }
            }
        }
        schemaManager.getAis(session);
    }

    @After
    public void tearDown() throws Exception {
        baseTearDown();
    }

    @Test
    public void insertCOIrows() throws Exception {
        final RowDef defaultRowDef = rowDefCache.getRowDef(DEFAULT_SCHEMA
                + ".customer");
        final RowDef test1RowDef = rowDefCache.getRowDef(TEST1_SCHEMA
                + ".customer");
        final RowDef test2RowDef = rowDefCache.getRowDef(TEST2_SCHEMA
                + ".customer");

        final RowData rowData = new RowData(new byte[1024]);
        final Object[] values = new Object[] { 1, "Acme Manufacturing" };

        rowData.createRow(defaultRowDef, values);
        store.writeRow(session, rowData);

        rowData.createRow(test1RowDef, values);
        store.writeRow(session, rowData);

        for (int cid = 1; cid <= 5; cid++) {
            values[0] = cid;
            rowData.createRow(test2RowDef, values);
            store.writeRow(session, rowData);
        }

        final byte[] columnBitMap = new byte[]{3};
        final int scanFlags = SCAN_FLAGS_START_AT_EDGE | SCAN_FLAGS_END_AT_EDGE;
        assertEquals(1, scanAllRows("default", defaultRowDef.getRowDefId(), scanFlags, null, null, columnBitMap, 0 ));
        assertEquals(1, scanAllRows("test1", test1RowDef.getRowDefId(), scanFlags, null, null, columnBitMap, 0 ));
        assertEquals(5, scanAllRows("test2", test2RowDef.getRowDefId(), scanFlags, null, null, columnBitMap, 0 ));
    }

    protected int scanAllRows(final String test, final int rowDefId,
            final int scanFlags, final RowData start, final RowData end,
            final byte[] columnBitMap, final int indexId) throws Exception {
        int scanCount = 0;
        result.clear();
        final RowCollector rc = store.newRowCollector(session, rowDefId,
                indexId, scanFlags, start, end, columnBitMap);

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
                p = rowData.getRowEnd();
            }
        }
        rc.close();

        return scanCount - (int) rc.getRepeatedRows();
    }
}
