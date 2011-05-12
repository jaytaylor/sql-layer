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
import com.akiban.sql.RegexFilenameFilter;
import com.akiban.sql.TestBase;

import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.test.it.ITBase;
import com.akiban.server.service.ServiceManagerImpl;
import com.akiban.server.service.config.Property;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class PostgresServerIT extends ITBase
{
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + PostgresServerIT.class.getPackage().getName().replace('.', '/'));

    public static final String SCHEMA_NAME = "user";
    public static final String DRIVER_NAME = "org.postgresql.Driver";
    public static final String CONNECTION_URL = "jdbc:postgresql://localhost:%d/user";
    public static final String USER_NAME = "user";
    public static final String USER_PASSWORD = "user";

    @Before
    public void loadDatabase() throws Exception {
        loadSchemaFile(new File(RESOURCE_DIR, "schema.ddl"));
        for (File data : RESOURCE_DIR.listFiles(new RegexFilenameFilter(".*\\.dat"))) {
            loadDataFile(data);
        }
    }

    protected void loadSchemaFile(File file) throws Exception {
        Reader rdr = null;
        try {
            rdr = new FileReader(file);
            BufferedReader brdr = new BufferedReader(rdr);
            String tableName = null;
            List<String> tableDefinition = new ArrayList<String>();
            while (true) {
                String line = brdr.readLine();
                if (line == null) break;
                line = line.trim();
                if (line.startsWith("CREATE TABLE "))
                    tableName = line.substring(13);
                else if (line.startsWith("("))
                    tableDefinition.clear();
                else if (line.startsWith(")"))
                    createTable(SCHEMA_NAME, tableName, 
                                tableDefinition.toArray(new String[tableDefinition.size()]));
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
                NewRow row = new NiceRow(tableId);
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

    protected Connection connection;

    @Before
    public void openConnection() throws Exception {
        int port = serviceManager().getPostgresService().getPort();
        if (port < 0) {
            throw new Exception("akserver.postgres.port is not set.");
        }
        String url = String.format(CONNECTION_URL, port);
        Class.forName(DRIVER_NAME);
        connection = DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    @After
    public void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            }
            catch (SQLException ex) {
            }
            connection = null;
        }
    }

    @Parameters
    public static Collection<Object[]> queries() throws Exception {
        return TestBase.sqlAndExpectedAndParams(RESOURCE_DIR);
    }

    protected String caseName, sql, expected;
    protected String[] params;

    public PostgresServerIT(String caseName, String sql, String expected, 
                            String[] params) {
        this.caseName = caseName;
        this.sql = sql.trim();
        this.expected = expected;
        this.params = params;
    }

    @Test
    public void testQuery() throws Exception {
        StringBuilder data = new StringBuilder();
        PreparedStatement stmt = connection.prepareStatement(sql);
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                stmt.setString(i + 1, params[i]);
            }
        }
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData md = rs.getMetaData();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            if (i > 1) data.append('\t');
            data.append(md.getColumnName(i));
        }
        data.append('\n');
        while (rs.next()) {
            for (int i = 1; i <= md.getColumnCount(); i++) {
                if (i > 1) data.append('\t');
                data.append(rs.getString(i));
            }
            data.append('\n');
        }
        stmt.close();
        assertEquals("Difference in " + caseName, expected, data.toString());
    }
}
