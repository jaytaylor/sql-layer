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
import com.foundationdb.ais.model.*;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.transaction.*;
import com.foundationdb.server.store.*;
import com.foundationdb.server.types.aksql.aktypes.AkGUID;

import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.sql.*;
import java.util.UUID;
import java.util.Arrays;
import java.util.Random;

public class BlobIT extends PostgresServerITBase {
    int dataSize = 100000;
    
    @Test
    public void testCleanUpLobs() throws Exception {
        Connection conn = getConnection();
        String idA =  UUID.randomUUID().toString();
        PreparedStatement pstmt = conn.prepareCall("CALL sys.create_specific_blob( ? )");
        pstmt.setString(1, idA);
        pstmt.execute();
        pstmt.close();
        
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        Assert.assertTrue(ls.existsLob(getTransaction(), idA));
        commit();
        conn.close();
        
        Thread.sleep(100L);
        
        Assert.assertFalse(ls.existsLob(getTransaction(), idA));
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] ids = new String[n];
        
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();
        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();
        
        LobService ls = serviceManager().getServiceByClass(LobService.class);
        
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids[k]));
        }
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.setBlob(4, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];
        String[] idsC = new String[n];

        
        stmt.execute("SELECT blA, blB, blC FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
            idsB[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
            idsC[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();
        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), idsA[k]));
            Assert.assertFalse(ls.existsLob(getTransaction(), idsB[k]));
            Assert.assertFalse(ls.existsLob(getTransaction(), idsC[k]));
        }
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];

        stmt.execute("SELECT bl, bl2 FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
            idsB[j] = rs.getString(2);
        }
        rs.close();
        stmt.execute(("TRUNCATE TABLE t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);

        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), idsA[k]));
            Assert.assertFalse(ls.existsLob(getTransaction(), idsB[k]));
        }
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t1[k]));
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
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
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }

        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids[k]));
        }
        commit();
    }

    @Test
    public void testDeleteLobsWithGroupD() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt.close();
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();

        stmt.execute(("DROP TABLE t2"));
        stmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(getTransaction(), ids_t1[k]));
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
    }

    @Test
    public void testDeleteLobsWithGroupE() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT id_blob(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT id_blob(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("ALTER TABLE t2 DROP GROUPING FOREIGN KEY"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(getTransaction(), ids_t1[k]));
            Assert.assertTrue(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
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
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt.close();
        String[] ids = new String[n];

        stmt.execute("SELECT bl FROM testx.t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();
        stmt.execute(("DROP SCHEMA testx CASCADE"));
        stmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids[k]));
        }
        commit();
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
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }

        stmt.execute(("DROP SCHEMA t CASCADE"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
    }

    @Test
    public void testDropLobColumnA() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt.close();
        String[] ids = new String[n];
        
        stmt.execute("SELECT bl FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }
        rs.close();
        stmt.execute(("ALTER TABLE t1 DROP COLUMN bl"));
        stmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids[k]));
        }
        commit();
    }

    @Test
    public void testDropLobColumnB() throws Exception {
        int n = 1;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB, col3 int)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?, 1)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt.close();
        String[] ids = new String[n];

        stmt.execute("SELECT id_blob(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("ALTER TABLE t1 DROP COLUMN col3"));
        stmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(getTransaction(), ids[k]));
        }
        commit();
    }


    @Test
    public void testDropLobColumnC() throws Exception {
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
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT bl_t2 FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = AkGUID.bytesToUUID(Arrays.copyOfRange(rs.getBytes(1), 1, 17), 0).toString();
        }

        stmt.execute(("ALTER TABLE t.t2 DROP COLUMN bl_t2"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
    }

    @Test
    public void testDropLobColumnD() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t.t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t.t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB, col4 INT)");
        stmt.execute("ALTER TABLE t.t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t.t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t.t1 VALUES (?)");
        for (int i = 0; i < n; i++) {
            pstmt.setInt(1, i);
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t.t2 VALUES (?,?,?,1)");
        for (int ii = 0; ii < n; ii++) {
            pstmt.setInt(1, ii * 10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT id_blob(bl_t2) FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("ALTER TABLE t.t2 DROP COLUMN col4"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
    }

    @Test
    public void testDropLobColumnE() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            pstmt.setInt(1, i);
            pstmt.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        pstmt = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            pstmt.setInt(1, ii*10);
            pstmt.setInt(2, ii);
            pstmt.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            pstmt.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT id_blob(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT id_blob(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("ALTER TABLE t1 DROP COLUMN bl"));
        stmt.close();
        pstmt.close();
        conn.close();

        LobService ls = serviceManager().getServiceByClass(LobService.class);
        TransactionContext tcx = serviceManager().getServiceByClass(FDBHolder.class).getTransactionContext();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse( ls.existsLob(getTransaction(), ids_t1[k]));
            Assert.assertTrue(ls.existsLob(getTransaction(), ids_t2[k]));
        }
        commit();
    }
    
    @Test
    public void createManyBlobsA() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        for (int i = 0; i < 100; i++) {
            stmt.execute("INSERT INTO t1 VALUES (" + i + ", create_long_blob())");
        }
        conn.close();
    }

    @Test
    public void createManyBlobsB() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        for (int i = 0; i < 100; i++) {
            stmt.execute("SELECT create_long_blob(unhex('050505'))");
        }
        conn.close();
    }

    @Test
    public void createLargeBlob() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        
        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES ( 1, ?)");
        int lengthInMb = 9;
        pstmt.setBlob(1, getInputStreamData(lengthInMb));
        pstmt.execute();
        
        ResultSet rs = stmt.executeQuery("SELECT bl from t1");
        rs.next();
        Blob blob = rs.getBlob(1);
        InputStream readStr = blob.getBinaryStream();
        InputStream dataStr = getInputStreamData(lengthInMb);
        int byteA;
        int byteB;
        while ( (byteA = readStr.read()) != -1) {
            byteB = dataStr.read();
            assert byteA == byteB;
        }
        conn.close();
    }

    @Test
    public void blobPerformanceA() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement pstmt = conn.prepareCall("INSERT INTO t1 VALUES ( 1, ?)");
        int lengthInMb = 9;
        long start = System.currentTimeMillis();
        pstmt.setBlob(1, getInputStreamData(lengthInMb) );
        long stop = System.currentTimeMillis();
        System.out.println("Writing --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");
        pstmt.execute();

        ResultSet rs = stmt.executeQuery("SELECT bl from t1");
        rs.next();
        Blob blob = rs.getBlob(1);
        InputStream readStr = blob.getBinaryStream();
        int byteA;
        byte[] out = new byte[lengthInMb*1000000];
        int i = 0;
        start = System.currentTimeMillis();
        while ( (byteA = readStr.read()) != -1) {
            out[i] = (byte)byteA;
        }
        stop = System.currentTimeMillis();
        System.out.println("Reading --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");        
        conn.close();
    }

    @Test
    public void blobPerformanceB() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        
        long start = System.currentTimeMillis();
        stmt.execute("INSERT INTO t1 VALUES (1, create_long_blob())");
        long stop = System.currentTimeMillis();
        System.out.println("Writing long blob without data --> time: " + ((stop - start)) + " ms");

        start = System.currentTimeMillis();
        stmt.execute("INSERT INTO t1 VALUES (2, create_short_blob())");
        stop = System.currentTimeMillis();
        System.out.println("Writing short blob without data --> time: " + ((stop - start)) + " ms");

        start = System.currentTimeMillis();
        stmt.execute("INSERT INTO t1 VALUES (3, create_long_blob(unhex('06')))");
        stop = System.currentTimeMillis();
        System.out.println("Writing long blob with 1 byte --> time: " + ((stop - start)) + " ms");

        start = System.currentTimeMillis();
        stmt.execute("INSERT INTO t1 VALUES (4, create_short_blob(unhex('07')))");
        stop = System.currentTimeMillis();
        System.out.println("Writing short blob with 1 byte --> time: " + ((stop - start)) + " ms");

        start = System.currentTimeMillis();
        stmt.execute("INSERT INTO t1 VALUES (0, create_long_blob())");
        stop = System.currentTimeMillis();
        System.out.println("Writing long blob without data --> time: " + ((stop - start)) + " ms");
        
        conn.close();
    }

    private Transaction getTransaction() {
        TransactionService txnService = txnService();
        if (txnService instanceof FDBTransactionService) {
            if ( txnService.isTransactionActive(session())) {
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            } else {
                txnService.beginTransaction(session());
                return ((FDBTransactionService) txnService).getTransaction(session()).getTransaction();
            }
        }
        else
            return null;
    }

    private void commit() {
        TransactionService ts = txnService();
        ts.commitOrRetryTransaction(session());
        ts.rollbackTransactionIfOpen(session());
    }
    
    
    private byte[] generateBytes(int length) {
        byte[] inp = new byte[length];
        Random random = new Random();
        random.nextBytes(inp);
        return inp;
    }
    
    private InputStream getInputStreamData(final int sizeInMB) {
        return new InputStream() {
            private int count1 = 0;
            private int count2 = 0;
            
            @Override
            public int read() throws IOException {
                count1++;
                if (count1 > 1000000) {
                    count2++;
                    count1 = 0;
                }
                return count2 < sizeInMB ? count1%256 : -1;
            }
        };
    }
}
