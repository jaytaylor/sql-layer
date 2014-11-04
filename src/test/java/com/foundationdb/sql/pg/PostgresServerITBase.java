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

import com.foundationdb.server.service.is.BasicInfoSchemaTablesService;
import com.foundationdb.server.service.is.BasicInfoSchemaTablesServiceImpl;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;

import static org.junit.Assert.fail;

import org.junit.AfterClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Ignore
public class PostgresServerITBase extends ITBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresServerITBase.class);
    public static final File RESOURCE_DIR = 
        new File("src/test/resources/"
                 + PostgresServerITBase.class.getPackage().getName().replace('.', '/'));

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:fdbsql://%s:%d/"+SCHEMA_NAME;
    public static final String USER_NAME = "auser";
    public static final String USER_PASSWORD = "apassword";

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                .bindAndRequire(BasicInfoSchemaTablesService.class, BasicInfoSchemaTablesServiceImpl.class)
                .bindAndRequire(PostgresService.class, PostgresServerManager.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(PostgresServerITBase.class);
    }

    /** Should include %d format specifier for port number */
    protected String getConnectionURL() {
        return CONNECTION_URL;
    }

    protected Connection openConnection() throws Exception {
        int port = getPostgresService().getPort();
        if (port <= 0) {
            throw new Exception("fdbsql.postgres.port is not set.");
        }
        String url = String.format(getConnectionURL(), getPostgresService().getHost(), port);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    protected static void closeConnection(Connection connection) throws Exception {
        if (!connection.isClosed())
            connection.close();
    }

    protected PostgresService getPostgresService() {
        return serviceManager().getServiceByClass(PostgresService.class);
    }

    protected PostgresServer server() {
        return getPostgresService().getServer();
    }

    private static final Callable<Void> forgetOnStopServices = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                forgetConnection();
                return null;
            }
        };

    // One element connection pool.
    private static final ThreadLocal<Connection> connectionRef = new ThreadLocal<>();

    protected Connection getConnection() throws Exception {
        Connection connection = connectionRef.get();
        if (connection == null || connection.isClosed()) {
            beforeStopServices.add(forgetOnStopServices);
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
                    LOG.error("caught an interrupted exception; re-interrupting", ex);
                    Thread.currentThread().interrupt();
                }
            }
            connection = openConnection();
            connectionRef.set(connection);
        }
        return connection;
    }

    public static void forgetConnection() throws Exception {
        Connection connection = connectionRef.get();
        if (connection != null) {
            closeConnection(connection);
            connectionRef.remove();
            beforeStopServices.remove(forgetOnStopServices);
        }
    }

    @AfterClass
    public static void closeConnection() throws Exception {
        forgetConnection();
    }

    protected PostgresServerITBase() {
    }

    protected List<List<?>> sql(String sql) {
        try {
            Connection conn = getConnection();
            try {
                try (Statement statement = conn.createStatement()) {
                    if (!statement.execute(sql))
                        return null;
                    List<List<?>> results = new ArrayList<>();
                    try (ResultSet rs = statement.getResultSet()) {
                        int ncols = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            List<Object> row = new ArrayList<>(ncols);
                            for (int i = 0; i < ncols; ++i)
                                row.add(rs.getObject(i+1));
                            results.add(row);
                        }
                    }
                    if (statement.getMoreResults())
                        throw new RuntimeException("multiple ResultSets for SQL: " + sql);
                    return results;
                }
            }
            finally {
                forgetConnection();
            }
        } catch (Exception e) {
            throw new RuntimeException("while executing SQL: " + sql, e);
        }
    }

}
