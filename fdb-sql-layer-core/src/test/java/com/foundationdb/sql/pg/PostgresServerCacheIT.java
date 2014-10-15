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

package com.foundationdb.sql.pg;

import com.foundationdb.server.api.dml.scan.NewRow;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.util.Collections;
import java.util.Map;

public class PostgresServerCacheIT extends PostgresServerFilesITBase
{
    public static final String QUERY = "SELECT id FROM t1 WHERE id = %d";
    public static final String PQUERY = "SELECT id FROM t1 WHERE id = ?";
    public static final int NROWS = 100;
    public static final String CAPACITY = "10";

    private int hitsBase;
    private int missesBase;
    
    @Override
    protected Map<String, String> startupConfigProperties() {
        return Collections.singletonMap("fdbsql.postgres.statementCacheCapacity", CAPACITY);
    }

    @Before
    public void createData() throws Exception {
        int tid = createTable(SCHEMA_NAME, "t1", "id int not null primary key");
        NewRow[] rows = new NewRow[NROWS];
        for (int i = 0; i < NROWS; i++) {
            rows[i] = createNewRow(tid, i);
        }
        writeRows(rows);
        hitsBase = server().getStatementCacheHits();
        missesBase = server().getStatementCacheMisses();
    }

    @Test
    public void testRepeated() throws Exception {
        Statement stmt = getConnection().createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i / NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 990, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 10, server().getStatementCacheMisses() - missesBase);
    }

    @Test
    public void testSequential() throws Exception {
        Statement stmt = getConnection().createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i % NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 0, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1000, server().getStatementCacheMisses() - missesBase);
    }

    @Test
    public void testPreparedRepeated() throws Exception {
        PreparedStatement stmt = getConnection().prepareStatement(PQUERY);
        for (int i = 0; i < 1000; i++) {
            pquery(stmt, i / NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 4, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1, server().getStatementCacheMisses() - missesBase);
    }
    
    @Test
    public void testPreparedSequential() throws Exception {
        PreparedStatement stmt = getConnection().prepareStatement(PQUERY);
        for (int i = 0; i < 1000; i++) {
            pquery(stmt, i % NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 4, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1, server().getStatementCacheMisses() - missesBase);
        
    }
    
    protected void query(Statement stmt, int n) throws Exception {
        ResultSet rs = stmt.executeQuery(String.format(QUERY, n));
        if (rs.next()) {
            assertEquals("Query result matches", n, rs.getInt(1));
        }
        else {
            fail("No query results");
        }
    }
    
    protected void pquery (PreparedStatement stmt, int n) throws Exception {
        stmt.setInt(1, n);
        stmt.execute();
        ResultSet rs = stmt.getResultSet();
        if (rs.next()) {
            assertEquals("Query Result Matches", n, rs.getInt(1));
        } else {
            fail ("No Query results");
            
        }
    }
}
