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

package com.akiban.sql.pg;

import com.akiban.server.service.config.Property;
import com.akiban.server.api.dml.scan.NewRow;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.SQLException;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

public class PostgresServerCacheIT extends PostgresServerFilesITBase
{
    public static final String QUERY = "SELECT id FROM t1 WHERE id = %d";
    public static final int NROWS = 100;
    public static final String CAPACITY = "10";

    private int hitsBase;
    private int missesBase;
    
    @Override
    protected Collection<Property> startupConfigProperties() {
        return Collections.singleton(new Property("akserver.postgres.statementCacheCapacity", CAPACITY));
    }

    @Before
    public void createData() throws Exception {
        int tid = createTable(SCHEMA_NAME, "t1", "id int primary key");
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
        Statement stmt = connection.createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i / NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 990, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 10, server().getStatementCacheMisses() - missesBase);
    }

    @Test
    public void testSequential() throws Exception {
        Statement stmt = connection.createStatement();
        for (int i = 0; i < 1000; i++) {
            query(stmt, i % NROWS);
        }
        stmt.close();
        assertEquals("Cache hits matches", 0, server().getStatementCacheHits() - hitsBase);
        assertEquals("Cache misses matches", 1000, server().getStatementCacheMisses() - missesBase);
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

}
