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

package com.foundationdb.server.test.it.bugs.bug701580;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.test.it.ITBase;
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

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);

        writeRows(
                row(t2, 1, "0"),
                row(t2, 2, "1"),
                row(t2, 3, "2")
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

        confirmIds("t1", 1, 2);
        confirmIds("t2", 1, 2);
        confirmIds("t3", 3, 2);
    }

    /**
     * Confirm that the given table has sequential index IDs starting from the given number, and that its
     * group table has all those indexes as well.
     * @param tableName the table to start at
     * @param startingAt the index to start at
     * @param expectedUIndexes how many indexes you expect on the user table
     * @throws Exception if there's a problem!
     */
    private void confirmIds(String tableName, int startingAt, int expectedUIndexes)
            throws Exception {
        Table table = ddl().getAIS(session()).getTable("test", tableName);

        Set<Integer> expectedUTableIds = new HashSet<>();
        Set<Integer> actualUTableIds = new HashSet<>();
        for (Index index : table.getIndexes()) {
            actualUTableIds.add(index.getIndexId());
            expectedUTableIds.add( expectedUTableIds.size() + startingAt );
        }

        assertEquals("table index count", expectedUIndexes, actualUTableIds.size());
    }
}
