/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.sql.pg;

import com.akiban.server.service.config.Property;
import com.akiban.server.api.dml.scan.NewRow;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.sql.ResultSet;
import java.sql.Statement;

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
        int tid = createTable(SCHEMA_NAME, "t1", "id int not null primary key");
        NewRow[] rows = new NewRow[NROWS];
        for (int i = 0; i < NROWS; i++) {
            rows[i] = createNewRow(tid, i);
        }
        writeRows(rows);
        server().cleanStatementCaches();
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
