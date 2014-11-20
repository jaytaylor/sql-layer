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

import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.test.it.ITBase;
import org.junit.Test;

import java.util.concurrent.Callable;

public class TableStatusRecoveryIT extends ITBase {
    private final static int ROW_COUNT = 100;

    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < ROW_COUNT; i++) {
            writeRows(row(tableId, i, "This is record # " + 1));
        }
        assertEquals(ROW_COUNT, getRowCount(tableId));

        safeRestartTestServices();

        assertEquals(ROW_COUNT, getRowCount(tableId));
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

    private long  getRowCount(final int tableId) {
        return txnService().run(session(), new Callable<Long>()
        {
            @Override
            public Long call() throws Exception {
                return getTable(tableId).rowDef().getTableStatus().getRowCount(session());
            }
        });
    }
}
