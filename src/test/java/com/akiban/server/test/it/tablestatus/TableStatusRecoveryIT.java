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

package com.akiban.server.test.it.tablestatus;

import static org.junit.Assert.assertEquals;

import java.util.Collections;

import com.akiban.server.test.it.ITBase;
import com.persistit.Transaction;
import com.persistit.exception.PersistitIOException;
import org.junit.Test;

import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatus;
import com.akiban.server.service.config.Property;
import com.persistit.Persistit;

public class TableStatusRecoveryIT extends ITBase {
    
    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < 10000; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }
        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts1.getRowCount());
        
        crashAndRestartSaveDatapath();
        
        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts2.getRowCount());

    }

    @Test
    public void pkLessInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT, V VARCHAR(255)");
        for (int i = 0; i < 10000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts1.getRowCount());

        for (int i = 10000; i < 20000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        
        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(20000, ts2.getRowCount());

        crashAndRestartSaveDatapath();

        final TableStatistics ts3 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(20000, ts3.getRowCount());

        // Transaction so we can directly read the table status
        final Transaction trx = treeService().getTransaction(session());
        trx.begin();
        try {
            final TableStatus status = store().getRowDefCache().getRowDef(tableId).getTableStatus();
            assertEquals(20000, status.getRowCount());
            writeRows(createNewRow(tableId, 20001, "This is record # ", -1));
            assertEquals(20001, status.getUniqueID());
            trx.commit();
        }
        finally {
            trx.end();
        }
    }

    @Test
    public void autoIncrementInsertTest() throws Exception {
        final int INSERT_COUNT = 10000;

        int tableId = createTable("test", "A", "I INT AUTO_INCREMENT, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 1; i <= INSERT_COUNT; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }

        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count before restart", INSERT_COUNT, ts1.getRowCount());
        assertEquals("auto inc before restart", INSERT_COUNT, ts1.getAutoIncrementValue());

        crashAndRestartSaveDatapath();

        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count after restart", INSERT_COUNT, ts2.getRowCount());
        assertEquals("auto inc after restart", INSERT_COUNT, ts2.getAutoIncrementValue());

    }

    private void crashAndRestartSaveDatapath() throws Exception {
        final Persistit db = serviceManager().getTreeService().getDb();

        final String datapath = db.getProperty("datapath");
        db.getJournalManager().force();
        crashTestServices();

        final Property property = new Property("akserver.datapath", datapath);
        restartTestServices(Collections.singleton(property));
    }
}
