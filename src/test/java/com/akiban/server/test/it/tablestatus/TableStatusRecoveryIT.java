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

import com.akiban.server.TableStatus;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import com.akiban.server.TableStatistics;
import com.akiban.server.PersistitTransactionalCacheTableStatus;
import com.akiban.server.service.config.Property;
import com.persistit.Persistit;

public class TableStatusRecoveryIT extends ITBase {
    
    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < 10000; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }
        final TableStatistics ts1 = store().getTableStatistics(session(), tableId);
        assertEquals(10000, ts1.getRowCount());
        
        final Persistit db = serviceManager().getTreeService().getDb();

        final String datapath = db.getProperty("datapath");
        db.getJournalManager().force();
        crashTestServices();
      
        final Property property = new Property("akserver.datapath", datapath);
        restartTestServices(Collections.singleton(property));
        
        final TableStatistics ts2 = store().getTableStatistics(session(), tableId);
        assertEquals(10000, ts2.getRowCount());

    }

    @Test
    public void pkLessInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT, V VARCHAR(255)");
        for (int i = 0; i < 10000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        final TableStatistics ts1 = store().getTableStatistics(session(), tableId);
        assertEquals(10000, ts1.getRowCount());
        
        final Persistit db = serviceManager().getTreeService().getDb();

        final String datapath = db.getProperty("datapath");
        
        db.checkpoint();

        for (int i = 10000; i < 20000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        
        final TableStatistics ts2 = store().getTableStatistics(session(), tableId);
        assertEquals(20000, ts2.getRowCount());
        db.getJournalManager().force();

        crashTestServices();
      
        final Property property = new Property("akserver.datapath", datapath);
        restartTestServices(Collections.singleton(property));
        
        final TableStatistics ts3 = store().getTableStatistics(session(), tableId);
        assertEquals(20000, ts3.getRowCount());
        final TableStatus status = store().getRowDefCache().getRowDef(tableId).getTableStatus();
        assertEquals(20000, status.getRowCount());
        assertEquals(20001, status.createNewUniqueID());
    }
}
