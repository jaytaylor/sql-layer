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

package com.akiban.server.test.it.bugs.bug701580;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.UserTable;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;

public final class SpuriousDuplicateKeyIT extends ITBase {
    @Test
    public void simpleOnce() throws Exception {
        simpleTestCase();
    }

    @Test
    public void simpleTwice() throws Exception {
        simpleTestCase();
        simpleTestCase();
    }

    private void simpleTestCase() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");
        int t2 = createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique(theme)");

        confirmIds("t1", 1, 2, 2);
        confirmIds("t2", 1, 2, 2);

        writeRows(
                createNewRow(t2, 1, "0"),
                createNewRow(t2, 2, "1"),
                createNewRow(t2, 3, "2")
        );
        dropAllTables();
    }

    @Test
    public void indexIdsLocalToGroup() throws Exception {
        createTable("test", "t1", "bid1 int not null, token varchar(64), primary key(bid1)");
        createIndex("test", "t1", "token", "token");

        createTable("test", "t2", "bid int not null, theme varchar(64), primary key (bid), unique (theme)");
        createTable("test", "t3", "id int not null primary key, bid_id int, "+
                    "GROUPING FOREIGN KEY (bid_id) REFERENCES t2 (bid)");
        createIndex("test", "t3", "__akiban_fk", "bid_id");

        confirmIds("t1", 1, 2, 2);
        confirmIds("t2", 1, 2, 4);
        confirmIds("t3", 3, 2, 4);
    }

    /**
     * Confirm that the given table has sequential index IDs starting from the given number, and that its
     * group table has all those indexes as well.
     * @param tableName the table to start at
     * @param startingAt the index to start at
     * @param expectedUIndexes how many indexes you expect on the user table
     * @param expectedGIndexes how many indexes you expect on the group table
     * @throws Exception if there's a problem!
     */
    private void confirmIds(String tableName, int startingAt, int expectedUIndexes, int expectedGIndexes)
            throws Exception {
        UserTable uTable = ddl().getAIS(session()).getUserTable("test", tableName);

        Set<Integer> expectedUTableIds = new HashSet<Integer>();
        Set<Integer> actualUTableIds = new HashSet<Integer>();
        for (Index index : uTable.getIndexes()) {
            actualUTableIds.add(index.getIndexId());
            expectedUTableIds.add( expectedUTableIds.size() + startingAt );
        }

        Set<Integer> actualGTableIds = new HashSet<Integer>();
        for(Index index : uTable.getGroup().getGroupTable().getIndexes()) {
            actualGTableIds.add(index.getIndexId());
        }

        assertEquals("uTable index count", expectedUIndexes, actualUTableIds.size());
        assertEquals("actualUTableIds", actualUTableIds, expectedUTableIds);

        if(!actualGTableIds.containsAll(actualUTableIds)) {
            Set<Integer> missing = new HashSet<Integer>(actualUTableIds);
            missing.removeAll(actualGTableIds);
            fail(String.format("missing %s: %s doesn't contain all of %s", missing, actualGTableIds, actualUTableIds));
        }
        assertEquals("gTable index count", expectedGIndexes, actualGTableIds.size());
    }
}
