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

import com.akiban.sql.TestBase;

import com.akiban.server.rowdata.RowDef;

import com.akiban.server.api.dml.scan.CursorId;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.api.dml.scan.RowOutput;
import com.akiban.server.api.dml.scan.ScanAllRequest;
import com.akiban.server.api.dml.scan.ScanFlag;
import com.akiban.server.test.it.ITBase;
import com.akiban.sql.RegexFilenameFilter;

import com.akiban.ais.model.Index.JoinType;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static junit.framework.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

@Ignore
public class PostgresServerITBase extends ITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerITBase.class);
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + PostgresServerITBase.class.getPackage().getName().replace('.', '/'));

    public static final String SCHEMA_NAME = "user";
    public static final String DRIVER_NAME = "org.postgresql.Driver";
    public static final String CONNECTION_URL = "jdbc:postgresql://localhost:%d/user";
    public static final String USER_NAME = "user";
    public static final String USER_PASSWORD = "user";

    public void loadDatabase(File dir) throws Exception {
        loadSchemaFile(new File(dir, "schema.ddl"));
        File groupIndex = new File(dir, "group.idx");
        if (groupIndex.exists())
            loadGroupIndexFile(groupIndex);
        for (File data : dir.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(data);
        }
    }

    protected int rootTableId;

    protected void loadSchemaFile(File file) throws Exception {
        Reader rdr = null;
        try {
            rdr = new FileReader(file);
            BufferedReader brdr = new BufferedReader(rdr);
            String tableName = null;
            List<String> tableDefinition = new ArrayList<String>();
            boolean first = true;
            while (true) {
                String line = brdr.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.startsWith("CREATE TABLE "))
                    tableName = line.substring(13);
                else if (line.startsWith("("))
                    tableDefinition.clear();
                else if (line.startsWith(")")) {
                    int id = createTable(SCHEMA_NAME, tableName, 
                                         tableDefinition.toArray(new String[tableDefinition.size()]));
                    if (first) {
                        rootTableId = id;
                        first = false;
                    }
                }
                else {
                    if (line.endsWith(","))
                        line = line.substring(0, line.length() - 1);
                    tableDefinition.add(line);
                }
            }
        }
        finally {
            if (rdr != null) {
                try {
                    rdr.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    protected void loadGroupIndexFile(File file) throws Exception {
        Reader rdr = null;
        try {
            rdr = new FileReader(file);
            BufferedReader brdr = new BufferedReader(rdr);
            while (true) {
                String line = brdr.readLine();
                if (line == null) break;
                String defn[] = line.split("\t");
                JoinType joinType = JoinType.LEFT;
                if (defn.length > 3)
                    joinType = JoinType.valueOf(defn[3]);
                createGroupIndex(defn[0], defn[1], defn[2], joinType);
            }
        }
        finally {
            if (rdr != null) {
                try {
                    rdr.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    protected void loadDataFile(File file) throws Exception {
        String tableName = file.getName().replace(".dat", "");
        int tableId = tableId(SCHEMA_NAME, tableName);
        Reader rdr = null;
        try {
            rdr = new FileReader(file);
            BufferedReader brdr = new BufferedReader(rdr);
            while (true) {
                String line = brdr.readLine();
                if (line == null) break;
                String[] cols = line.split("\t");
                NewRow row = new NiceRow(tableId, store());
                for (int i = 0; i < cols.length; i++)
                    row.put(i, cols[i]);
                dml().writeRow(session(), row);
            }
        }
        finally {
            if (rdr != null) {
                try {
                    rdr.close();
                }
                catch (IOException ex) {
                }
            }
        }
    }

    protected String dumpData() throws Exception {
        final StringBuilder str = new StringBuilder();
        CursorId cursorId = dml()
            .openCursor(session(), aisGeneration(), 
                        new ScanAllRequest(rootTableId, null, 0,
                                           EnumSet.of(ScanFlag.DEEP)));
        dml().scanSome(session(), cursorId,
                       new RowOutput() {
                           public void output(NewRow row) {
                               RowDef rowDef = row.getRowDef();
                               str.append(rowDef.table().getName().getTableName());
                               for (int i = 0; i < rowDef.getFieldCount(); i++) {
                                   str.append(",");
                                   str.append(row.get(i));
                               }
                               str.append("\n");
                           }

                           public void mark() {
                           }
                           public void rewind() {
                           }
                       });
        dml().closeCursor(session(), cursorId);
        return str.toString();
    }

    protected Connection openConnection() throws Exception {
        int port = serviceManager().getPostgresService().getPort();
        if (port <= 0) {
            throw new Exception("akserver.postgres.port is not set.");
        }
        String url = String.format(CONNECTION_URL, port);
        Class.forName(DRIVER_NAME);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    protected void closeConnection(Connection Connection) throws Exception {
        connection.close();
    }

    protected PostgresServer server() {
        return serviceManager().getPostgresService().getServer();
    }

    protected Connection connection;

    @Before
    public void openTheConnection() throws Exception {
        for (int i = 0; i < 6; i++) {
            if (server().isListening())
                break;
            if (i == 1)
                LOG.warn("Postgres server not listening. Waiting...");
            else if (i == 5)
                fail("Postgres server still not listening. Giving up.");
            try {
                Thread.sleep(200);
            }
            catch (InterruptedException ex) {
                LOG.warn("caught an interrupted exception; re-interrupting", ex);
                Thread.currentThread().interrupt();
            }
        }
        connection = openConnection();
    }

    @After
    public void closeTheConnection() throws Exception {
        if (connection != null) {
            closeConnection(connection);
            connection = null;
        }
    }

    protected String caseName, sql, expected, error;
    protected String[] params;

    /** Parameterized version. */
    protected PostgresServerITBase(String caseName, String sql, 
                                   String expected, String error,
                                   String[] params) {
        this.caseName = caseName;
        this.sql = sql.trim();
        this.expected = expected;
        this.error = error;
        this.params = params;
    }

    protected PostgresServerITBase() {
    }

    protected void generateAndCheckResult() throws Exception {
        TestBase.generateAndCheckResult((TestBase.GenerateAndCheckResult)this, 
                                        caseName, expected, error);
    }

}
