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

import com.foundationdb.*;
import com.foundationdb.server.service.blob.LobRoutines;
import com.foundationdb.server.service.blob.LobService;

import com.foundationdb.server.store.*;
import org.junit.*;

import java.io.*;
import java.sql.*;
import java.util.*;

public class BlobIT extends PostgresServerITBase {
    
    @Test
    public void testCleanUpLobs() throws Exception {
        Connection conn = getConnection();
        String idA =  UUID.randomUUID().toString();
        PreparedStatement pstmt = conn.prepareCall("CALL sys.create_specific_blob( ? )");
        pstmt.setString(1, idA);
        pstmt.execute();
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        Assert.assertTrue(ls.existsLob(idA));
        conn.close();
        // time out is needed to have close finalize properly 
        Thread.sleep(10);
        Assert.assertFalse(ls.existsLob(idA));        
    }

    @Test
    public void testDeleteLobsWithTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        
        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        String[] ids = new String[n];
        
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();
        
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids[k]));
        }
    }

    @Test
    public void testDeleteLobsWithTableMultipleColumns() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, blA BLOB, blB BLOB, blC BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?,?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.setBlob(4, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];
        String[] idsC = new String[n];

        
        stmt.execute("SELECT blA, blB, blC FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = rs.getString(1);
            idsB[j] = rs.getString(1);
            idsC[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(idsA[k]));
            Assert.assertFalse(ls.existsLob(idsB[k]));
            Assert.assertFalse(ls.existsLob(idsC[k]));
        }
    }

    @Test
    public void testTruncateLobsWithTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB, bl2 BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];

        stmt.execute("SELECT bl, bl2 FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = rs.getString(1);
            idsB[j] = rs.getString(2);
        }
        rs.close();
        stmt.execute(("TRUNCATE TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(idsA[k]));
            Assert.assertFalse(ls.existsLob(idsB[k]));
        }
    }

    @Test
    public void testDeleteLobsWithGroupA() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");
        
        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids_t1[k]));
            Assert.assertFalse(ls.existsLob(ids_t2[k]));
        }
    }

    @Test
    public void testDeleteLobsWithGroupB() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids_t2[k]));
        }
    }

    @Test
    public void testDeleteLobsWithGroupC() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids[k]));
        }
    }
    
    @Test
    public void testDeleteLobsWithSchema() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE testx.t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO testx.t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }
        pstmt.close();
        String[] ids = new String[n];

        stmt.execute("SELECT bl FROM testx.t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("DROP SCHEMA testx CASCADE"));
        stmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids[k]));
        }
    }

    @Test
    public void testDropSchemaLobsWithGroup() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t.t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t.t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t.t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t.t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t.t1 VALUES (?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t.t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(1000)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("DROP SCHEMA t CASCADE"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(ids_t2[k]));
        }
    }
    
    private byte[] generateBytes(int length) {
        byte[] inp = new byte[length];
        Random random = new Random();
        random.nextBytes(inp);
        return inp;
    }
}
