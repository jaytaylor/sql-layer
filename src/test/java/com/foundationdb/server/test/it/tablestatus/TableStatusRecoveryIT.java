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

package com.foundationdb.server.test.it.tablestatus;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.Callable;

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import com.foundationdb.server.TableStatistics;
import com.foundationdb.server.TableStatus;

public class TableStatusRecoveryIT extends ITBase {
    private final static int ROW_COUNT = 100;

    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < ROW_COUNT; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }
        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(ROW_COUNT, ts1.getRowCount());

        safeRestartTestServices();
        
        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(ROW_COUNT, ts2.getRowCount());
    }

    @Test
    public void autoIncrementInsertTest() throws Exception {
        NewAISBuilder builder = AISBBasedBuilder.create("test", ddl().getTypesTranslator());
        builder.table("A").autoIncInt("I", 1).colString("V", 255).pk("I");
        ddl().createTable(session(), builder.ais().getTable("test", "A"));
        updateAISGeneration();

        int tableId = tableId("test", "A");
        for (int i = 1; i <= ROW_COUNT; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }

        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count before restart", ROW_COUNT, ts1.getRowCount());
        assertEquals("auto inc before restart", ROW_COUNT, ts1.getAutoIncrementValue());

        safeRestartTestServices();

        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count after restart", ROW_COUNT, ts2.getRowCount());
        assertEquals("auto inc after restart", ROW_COUNT, ts2.getAutoIncrementValue());
    }

    @Test
    public void ordinalCreationTest() throws Exception {
        final int aId = createTable("test", "A", "ID INT NOT NULL, PRIMARY KEY(ID)");
        final int aOrdinal = getOrdinal(aId);

        final int bId = createTable("test", "B", "ID INT NOT NULL, AID INT, PRIMARY KEY(ID)", akibanFK("AID", "A", "ID"));
        final int bOrdinal = getOrdinal(bId);
        
        assertEquals("ordinals unique before restart", true, aOrdinal != bOrdinal);

        safeRestartTestServices();

        assertEquals("parent ordinal same after restart", aOrdinal, getOrdinal(aId));
        assertEquals("child ordinal same after restart", bOrdinal, getOrdinal(bId));
        
        final int cId = createTable("test", "C", "ID INT NOT NULL, BID INT, PRIMARY KEY(ID)", akibanFK("BID", "B", "ID"));
        final int cOrdinal = getOrdinal(cId);
        
        assertEquals("new grandchild after restart has unique ordinal", true, cOrdinal != aOrdinal && cOrdinal != bOrdinal);
    }

    private int getOrdinal(final int tableId) throws Exception {
        return getTable(tableId).getOrdinal();
    }
}
