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

import com.akiban.ais.model.TableName;
import com.akiban.server.error.ErrorCode;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.PGStatement;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class PostgresServerPreparedStatementIT extends PostgresServerITBase {
    private static final String SCHEMA = "test";
    private static final String TABLE = "t";
    private static final TableName TABLE_NAME = new TableName(SCHEMA, TABLE);
    private static final int ROW_COUNT = 1;

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
        assertNotNull("Found new index", ais().getUserTable(TABLE_NAME).getIndex("x"));
    }

    @Test
    public void dropIndex() throws Exception {
        createIndex(SCHEMA, TABLE, "x", "x");
        PreparedStatement p = newDropIndex();
        int count = p.executeUpdate();
        assertEquals("Count from drop index", 0, count);
        assertNull("Index is gone", ais().getUserTable(TABLE_NAME).getIndex("x"));
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
}
