/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.ais.util.TableChange;
import com.foundationdb.server.service.blob.LobService;
import com.foundationdb.server.service.is.*;
import com.foundationdb.server.service.servicemanager.*;
import com.foundationdb.server.service.transaction.*;
import com.foundationdb.server.store.*;

import org.junit.*;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class BlobIT extends PostgresServerITBase {
    int dataSize = 100000;

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(ServerSchemaTablesService.class, ServerSchemaTablesServiceImpl.class);
    }
    
    @Test
    public void testCleanUpLobs() throws Exception {
        Connection conn = getConnection();
        UUID idA =  UUID.randomUUID();
        conn.setAutoCommit(false);
        PreparedStatement preparedStatement = conn.prepareCall("CALL sys.create_specific_blob( ? )");
        preparedStatement.setString(1, idA.toString());
        preparedStatement.execute();
        ResultSet rs = preparedStatement.getResultSet();
        rs.next();
        String idOut = rs.getObject(1).toString();
        Assert.assertTrue(idA.toString().equals(idOut));
        preparedStatement.close();
        conn.commit();
        conn.close();
        getAndOrBeginTransaction();
        LobService ls = lobService();
        Assert.assertFalse(ls.existsLob(session(), idA));
        commitOrRollback();
    }

    @Test
    public void testDropTableWithBLob() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        
        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids = new String[n];
        
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
        
        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropTableWithMultipleBlobColumns() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, blA BLOB, blB BLOB, blC BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?,?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.setBlob(4, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];
        String[] idsC = new String[n];

        
        stmt.execute("SELECT blob_id(blA), blob_id(blB), blob_id(blC) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = rs.getString(1);
            idsB[j] = rs.getString(2);
            idsC[j] = rs.getString(3);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();

        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(idsA[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(idsB[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(idsC[k])));
        }
        commitOrRollback();

        stmt.execute(("DROP TABLE t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(idsA[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(idsB[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(idsC[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testTruncateLobsWithTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB, bl2 BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] idsA = new String[n];
        String[] idsB = new String[n];

        stmt.execute("SELECT blob_id(bl), blob_id(bl2) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            idsA[j] = rs.getString(1);
            idsB[j] = rs.getString(2);
        }
        rs.close();
        
        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(idsA[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(idsB[k])));
        }
        commitOrRollback();

        stmt.execute(("TRUNCATE TABLE t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(idsA[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(idsB[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropGroupParentAndChildHaveBlobs() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");
        
        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
        
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();
        
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropGroupChildHasBlob() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        LobService ls = lobService();
        for (int k = 0; k < n; k++) {
            getAndOrBeginTransaction();
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
        
        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropGroupSingleTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();

        stmt.execute(("DROP GROUP t1"));
        stmt.close();
        preparedStatement.close();
        conn.close();
        
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropChildTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();
        
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
        
        stmt.execute(("DROP TABLE t2"));
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropGroupingForeinKey() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("ALTER TABLE t2 DROP GROUPING FOREIGN KEY"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }
    
    @Test
    public void testDropSchemaWithSingleTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE testx.t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO testx.t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM testx.t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);;
        }
        rs.close();

        LobService ls = lobService();
        for (int k = 0; k < n; k++) {
            getAndOrBeginTransaction();
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();

        stmt.execute(("DROP SCHEMA testx CASCADE"));
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropSchemaLobsWithGroup() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t.t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t.t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t.t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t.t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t.t1 VALUES (?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t.t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
        
        stmt.execute(("DROP SCHEMA t CASCADE"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropLobColumn() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();
        String[] ids = new String[n];
        
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
        
        stmt.execute(("ALTER TABLE t1 DROP COLUMN bl"));
        stmt.close();
        conn.close();
        
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropNonLobColumn() throws Exception {
        int n = 1;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB, col3 int)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?, 1)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();
        stmt.execute(("ALTER TABLE t1 DROP COLUMN col3"));
        stmt.close();
        conn.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }


    @Test
    public void testDropLobColumnInChildTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t.t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t.t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t.t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t.t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t.t1 VALUES (?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t.t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    
        
        stmt.execute(("ALTER TABLE t.t2 DROP COLUMN bl_t2"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropNonLobColumnInChildTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t.t1 (id INT PRIMARY KEY)");
        stmt.execute("CREATE TABLE t.t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB, col4 INT)");
        stmt.execute("ALTER TABLE t.t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t.t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t.t1 VALUES (?)");
        for (int i = 0; i < n; i++) {
            preparedStatement.setInt(1, i);
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t.t2 VALUES (?,?,?,1)");
        for (int ii = 0; ii < n; ii++) {
            preparedStatement.setInt(1, ii * 10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t.t2");
        ResultSet rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        stmt.execute(("ALTER TABLE t.t2 DROP COLUMN col4"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDropLobColumnInParentTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii*10);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
        
        stmt.execute(("ALTER TABLE t1 DROP COLUMN bl"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertFalse( ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
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
        
        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES ( 1, ?)");
        int lengthInMb = 9;
        preparedStatement.setBlob(1, getInputStreamData(lengthInMb));
        preparedStatement.execute();
        
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
    public void testDeleteRowsFromRoot() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();

        stmt.execute(("DELETE FROM t1 WHERE id > 2"));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 3; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        for (int k = 3; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDeleteRowsFromChildTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute(("DELETE FROM t2 where id_t2 > 2"));
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 3; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testDeleteRowsFromParentTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute(("DELETE FROM t1 where id > 2"));
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 3; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }

        for (int k = 3; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testUpdateRowFromSingleTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();

        stmt.execute(("UPDATE t1 SET bl = create_long_blob(unhex('010203')) WHERE id = 2"));
        stmt.execute("SELECT blob_id(bl), isTrue(unwrap_blob(bl) = unhex('010203')) from t1 WHERE id = 2");
        ResultSet resultSet = stmt.getResultSet();
        resultSet.next();
        String blobId = resultSet.getString(1);
        Assert.assertTrue(resultSet.getBoolean(2));
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(blobId)));
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testUpdateRowsFromSingleTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        String[] ids = new String[n];

        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids[j] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        commitOrRollback();

        stmt.execute(("UPDATE t1 SET bl = create_long_blob(unhex('010203')) WHERE id > 2"));
        stmt.execute("SELECT blob_id(bl) FROM t1 WHERE id > 2");
        ResultSet resultSet = stmt.getResultSet();
        String[] ids_new = new String[n-3];
        for (int j = 3; j < n; j++) {
            resultSet.next();
            ids_new[j-3] = resultSet.getString(1);
        }
        stmt.close();
        preparedStatement.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k <= 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids[k])));
        }
        for (int k = 3; k < n; k++) {
            Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_new[k-3])));
        }
        commitOrRollback();
    }
    
    @Test
    public void testUpdateRowFromChildTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute("UPDATE t2 SET bl_t2 = create_long_blob(unhex('030405')) WHERE id_t2 = 2");
        stmt.execute("SELECT blob_id(bl_t2) FROM t2 WHERE id_t2 = 2");
        ResultSet resultSet = stmt.getResultSet();
        resultSet.next();
        String blobId = resultSet.getString(1);
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(blobId)));
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testUpdateRowFromChildTableProtobuf() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB) STORAGE_FORMAT protobuf");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB) STORAGE_FORMAT protobuf");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrStartTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute("UPDATE t2 SET bl_t2 = create_long_blob(unhex('030405')) WHERE id_t2 = 2");
        stmt.execute("SELECT blob_id(bl_t2) FROM t2 WHERE id_t2 = 2");
        ResultSet resultSet = stmt.getResultSet();
        resultSet.next();
        String blobId = resultSet.getString(1);
        stmt.close();
        conn.close();

        getAndOrStartTransaction();
        for (int k = 0; k < 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(blobId)));
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }

    @Test
    public void testUpdateRowFromChildTableColumnKeys() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB) STORAGE_FORMAT column_keys");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB) STORAGE_FORMAT column_keys");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrStartTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute("UPDATE t2 SET bl_t2 = create_long_blob(unhex('030405')) WHERE id_t2 = 2");
        stmt.execute("SELECT blob_id(bl_t2) FROM t2 WHERE id_t2 = 2");
        ResultSet resultSet = stmt.getResultSet();
        resultSet.next();
        String blobId = resultSet.getString(1);
        stmt.close();
        conn.close();

        getAndOrStartTransaction();
        for (int k = 0; k < 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t2[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(blobId)));
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }
    
    @Test
    public void testUpdateRowFromParentTable() throws Exception {
        int n = 5;
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");
        stmt.execute("CREATE TABLE t2 (id_t2 INT PRIMARY KEY, id_t1 INT, bl_t2 BLOB)");
        stmt.execute("ALTER TABLE t2 ADD GROUPING FOREIGN KEY (id_t1) REFERENCES t1(id)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES (?,?)");
        for (int i = 0; i < n; i++ ) {
            preparedStatement.setInt(1, i);
            preparedStatement.setBlob(2, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement = conn.prepareCall("INSERT INTO t2 VALUES (?,?,?)");
        for (int ii = 0; ii < n; ii++ ) {
            preparedStatement.setInt(1, ii);
            preparedStatement.setInt(2, ii);
            preparedStatement.setBlob(3, new ByteArrayInputStream(generateBytes(dataSize)));
            preparedStatement.execute();
        }
        preparedStatement.close();

        String[] ids_t1 = new String[n];
        stmt.execute("SELECT blob_id(bl) FROM t1");
        ResultSet rs = stmt.getResultSet();
        for (int j = 0; j < n; j++) {
            rs.next();
            ids_t1[j] = rs.getString(1);
        }
        rs.close();

        String[] ids_t2 = new String[n];
        stmt.execute("SELECT blob_id(bl_t2) FROM t2");
        rs = stmt.getResultSet();
        for (int jj = 0; jj < n; jj++) {
            rs.next();
            ids_t2[jj] = rs.getString(1);
        }
        rs.close();

        LobService ls = lobService();
        getAndOrBeginTransaction();
        for (int k = 0; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();

        stmt.execute("UPDATE t1 SET bl = create_long_blob(unhex('030405')) WHERE id = 2");
        stmt.execute("SELECT blob_id(bl), isTrue(unwrap_blob(bl) = unhex('030405')) FROM t1 WHERE id = 2");
        ResultSet resultSet = stmt.getResultSet();
        Assert.assertTrue(resultSet.next());
        String blobId = resultSet.getString(1);
        Assert.assertTrue(resultSet.getBoolean(2));
        stmt.close();
        conn.close();

        getAndOrBeginTransaction();
        for (int k = 0; k < 2; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[2])));
        Assert.assertFalse(ls.existsLob(session(), UUID.fromString(ids_t1[2])));
        Assert.assertTrue(ls.existsLob(session(), UUID.fromString(blobId)));
        for (int k = 3; k < n; k++) {
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t1[k])));
            Assert.assertTrue(ls.existsLob(session(), UUID.fromString(ids_t2[k])));
        }
        commitOrRollback();
    }



    @Test (expected=AssertionError.class)
    public void dropMultipleBlobColumns() throws Exception {
        final String schema = "test";
        final String table = "t1";
        TableName tableName = new TableName(schema, table);

        final int tId = createTable(schema, table, "id INT PRIMARY KEY, bl_1 BLOB, bl_2 BLOB");
        Statement stmt = getConnection().createStatement();
        stmt.execute("INSERT INTO t1 VALUES (1, create_long_blob(unhex('010203')), create_long_blob(unhex('020304')))");
        stmt.close();

        TestAISBuilder builder = new TestAISBuilder(typesRegistry());
        builder.table(schema, table);
        builder.column(schema, table, "id", 0, "MCOMPAT", "int", false);
        builder.pk(schema, table);
        builder.indexColumn(schema, table, Index.PRIMARY, "id", 0, true, null);
        builder.basicSchemaIsComplete();
        builder.createGroup(table, schema);
        builder.addTableToGroup(tableName, schema, table);
        builder.groupingIsComplete();
        Table newTable = builder.akibanInformationSchema().getTable(schema, table);

        List<TableChange> changes = new ArrayList<>();
        changes.add(TableChange.createDrop("bl_1"));
        changes.add(TableChange.createDrop("bl_2"));
        List<com.foundationdb.ais.util.TableChange> indexChanges = new ArrayList<>();
        
        // throws an assert in OnlineHelper checkForDropLob
        ddl().alterTable(session(), tableName, newTable, changes, indexChanges, queryContext(newStoreAdapter(session())));
    }

    @Test
    public void protobufSDInUnwrappedMode() throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute("CREATE TABLE t2 (id INT PRIMARY KEY, bl_1 BLOB, bl_2 BLOB) STORAGE_FORMAT protobuf");
        stmt.execute("INSERT INTO t2 VALUES (1, create_long_blob(unhex('010203')), create_short_blob(unhex('020304')))");
        stmt.close();
        
        if (configService().getProperty("fdbsql.blob.return_unwrapped").equalsIgnoreCase("true")){
            stmt = getConnection().createStatement();
            stmt.execute("SELECT bl_1, bl_2 FROM t2");
            ResultSet rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            byte[] out1 = rs.getBytes(1);
            byte[] out2 = rs.getBytes(2);
            Assert.assertArrayEquals(new byte[]{0x01,0x02,0x03}, out1);
            Assert.assertArrayEquals(new byte[]{0x02,0x03,0x04}, out2);
            stmt.close();
        }
    }
    
    @Test
    public void columnKeysInUnwrappedMode() throws Exception {
        Statement stmt = getConnection().createStatement();
        stmt.execute("CREATE TABLE t2 (id INT PRIMARY KEY, bl_1 BLOB, bl_2 BLOB) STORAGE_FORMAT column_keys");
        stmt.execute("INSERT INTO t2 VALUES (1, create_long_blob(unhex('010203')), create_short_blob(unhex('020304')))");
        stmt.close();

        if (configService().getProperty("fdbsql.blob.return_unwrapped").equalsIgnoreCase("true")){
            stmt = getConnection().createStatement();
            stmt.execute("SELECT bl_1, bl_2 FROM t2");
            ResultSet rs = stmt.getResultSet();
            Assert.assertTrue(rs.next());
            byte[] out1 = rs.getBytes(1);
            byte[] out2 = rs.getBytes(2);
            Assert.assertArrayEquals(new byte[]{0x01,0x02,0x03}, out1);
            Assert.assertArrayEquals(new byte[]{0x02,0x03,0x04}, out2);
            stmt.close();
        }
    }
    
    //@Test
    public void blobPerformanceA() throws Exception {
        Connection conn = getConnection();
        Statement stmt = conn.createStatement();
        stmt.execute("CREATE TABLE t1 (id INT PRIMARY KEY, bl BLOB)");

        PreparedStatement preparedStatement = conn.prepareCall("INSERT INTO t1 VALUES ( 1, ?)");
        int lengthInMb = 9;
        long start = System.currentTimeMillis();
        preparedStatement.setBlob(1, getInputStreamData(lengthInMb) );
        long stop = System.currentTimeMillis();
        System.out.println("Writing --> time: " + ((stop - start)) + "ms, speed: " + (1000*(new Float(lengthInMb)/(stop-start)))+ " MB/sec");
        preparedStatement.execute();

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

    //@Test
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

    private Transaction getAndOrBeginTransaction() {
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
            Assert.fail();
        return null;
    }

    private void commitOrRollback() {
        TransactionService ts = txnService();
        try {
            ts.commitTransaction(session());
        } finally {
            ts.rollbackTransactionIfOpen(session());
        }
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
