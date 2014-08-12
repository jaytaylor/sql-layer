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

import com.foundationdb.ais.model.TableName;
import com.foundationdb.server.error.ErrorCode;
import org.junit.Before;
import org.junit.Test;
import com.foundationdb.sql.jdbc.PGStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

public class PostgresServerPreparedStatementIT extends PostgresServerITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final int ROW_COUNT = 5;

    @Before
    public void createAndInsert() {
        int tid = createTable(TABLE_NAME, "id int not null primary key, x int");
        for(long i = 1; i <= ROW_COUNT; ++i) {
            writeRow(tid, i, i*10);
        }
    }

    PreparedStatement newDropTable() throws Exception {
        return getConnection().prepareStatement("DROP TABLE "+TABLE_NAME);
    }

    PreparedStatement newCreateTable() throws Exception {
        return getConnection().prepareStatement("CREATE TABLE "+TABLE_NAME+"2(id int)");
    }

    PreparedStatement newCreateIndex() throws Exception {
        return getConnection().prepareStatement("CREATE INDEX x ON "+TABLE_NAME+"(x)");
    }

    PreparedStatement newDropIndex() throws Exception {
        return getConnection().prepareStatement("DROP INDEX x");
    }

    PreparedStatement newScan() throws Exception {
        PreparedStatement p = getConnection().prepareStatement("SELECT * FROM "+TABLE_NAME);
        // driver has lower bound of usage before fully using a named statement, this drops that
        if(p instanceof PGStatement) {
            PGStatement pgp = (PGStatement) p;
            pgp.setPrepareThreshold(1);
        }
        return p;
    }

    PreparedStatement newInsert() throws Exception {
        return getConnection().prepareStatement("INSERT INTO "+TABLE_NAME+" VALUES (100,1000)");
    }

    private static int countRows(ResultSet rs) throws SQLException {
        int count = 0;
        while(rs.next()) {
            ++count;
        }
        rs.close();
        return count;
    }

    private static List<List<Object>> listRows(ResultSet rs, int ncols) throws SQLException {
        List<List<Object>> rows = new ArrayList<>();
        while (rs.next()) {
            List<Object> row = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                row.add(rs.getObject(i+1));
            }
            rows.add(row);
        }
        rs.close();
        return rows;
    }

    private static void expectStale(PreparedStatement p) {
        try {
            p.executeQuery();
            fail("Expected exception");
        } catch(SQLException e) {
            assertEquals("Error code from exception", ErrorCode.STALE_STATEMENT.getFormattedValue(), e.getSQLState());
        }
    }

    @Test
    public void fullScan() throws Exception {
        PreparedStatement p = newScan();
        ResultSet rs = p.executeQuery();
        assertEquals("Scanned row count", ROW_COUNT, countRows(rs));
        p.close();
    }

    @Test
    public void singleRowInsert() throws Exception {
        PreparedStatement p = newInsert();
        int count = p.executeUpdate();
        assertEquals("Inserted count", 1, count);
        p.close();
    }

    @Test
    public void createIndex() throws Exception {
        PreparedStatement p = newCreateIndex();
        int count = p.executeUpdate();
        assertEquals("Count from create index", 0, count);
        assertNotNull("Found new index", ais().getTable(TABLE_NAME).getIndex("x"));
    }

    @Test
    public void dropIndex() throws Exception {
        createIndex(SCHEMA, TABLE, "x", "x");
        PreparedStatement p = newDropIndex();
        int count = p.executeUpdate();
        assertEquals("Count from drop index", 0, count);
        assertNull("Index is gone", ais().getTable(TABLE_NAME).getIndex("x"));
    }

    @Test
    public void dropTableInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pDrop = newDropTable();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pDrop.executeUpdate();
        assertEquals("Count from drop table", 0, count);
        expectStale(pScan);
        pScan.close();
        pDrop.close();
    }

    //
    // Two below aren't exactly desirable, but confirming expected behavior
    //

    @Test
    public void createTableInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pCreate = newCreateTable();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pCreate.executeUpdate();
        assertEquals("Count from create table", 0, count);
        expectStale(pScan);
        pScan.close();
        pCreate.close();
    }

    @Test
    public void createIndexInvalidates() throws Exception {
        PreparedStatement pScan = newScan();
        PreparedStatement pCreate = newCreateIndex();
        assertEquals("Row count from scan1", ROW_COUNT, countRows(pScan.executeQuery()));
        int count = pCreate.executeUpdate();
        assertEquals("Count from create index", 0, count);
        expectStale(pScan);
        pScan.close();
        pCreate.close();
    }

    @Test
    public void fetchSize() throws Exception {
        boolean ac = getConnection().getAutoCommit();
        getConnection().setAutoCommit(false);
        PreparedStatement p = newScan();
        ResultSet rs = p.executeQuery();
        List<List<Object>> rows = listRows(rs, 2);
        p.setFetchSize(2);
        rs = p.executeQuery();
        assertEquals("Rows with fetch size 2", rows, listRows(rs, 2));
        getConnection().setAutoCommit(ac);
    }
}
