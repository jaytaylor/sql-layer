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

import com.akiban.server.test.it.ITBase;

import org.junit.After;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;

import java.io.File;

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

    protected Connection connection;

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

    protected PostgresServerITBase() {
    }
}
