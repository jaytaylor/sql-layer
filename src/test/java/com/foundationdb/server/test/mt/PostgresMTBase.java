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

package com.foundationdb.server.test.mt;

import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.sql.pg.PostgresServer;
import com.foundationdb.sql.pg.PostgresServerManager;
import com.foundationdb.sql.pg.PostgresService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.fail;

public class PostgresMTBase extends MTBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresMTBase.class);

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:fdbsql://localhost:%d/"+SCHEMA_NAME;
    public static final String USER_NAME = "auser";
    public static final String USER_PASSWORD = "apassword";

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(PostgresService.class, PostgresServerManager.class);
    }

    @Override
    protected Map<String, String> startupConfigProperties() {
        return uniqueStartupConfigProperties(PostgresMTBase.class);
    }

    protected Connection createConnection() throws SQLException {
        PostgresServer server = serviceManager().getServiceByClass(PostgresService.class).getServer();
        int retry = 0;
        while(!server.isListening()) {
            if(retry == 1) {
                LOG.warn("Postgres server not listening. Waiting...");
            } else if(retry == 5) {
                fail("Postgres server still not listening. Giving up.");
            }
            try {
                Thread.sleep(200);
            } catch(InterruptedException ex) {
                LOG.error("caught an interrupted exception; re-interrupting", ex);
                Thread.currentThread().interrupt();
            }
            ++retry;
        }
        int port = server.getPort();
        if(port <= 0) {
            throw new IllegalStateException("port not set.");
        }
        String url = String.format(CONNECTION_URL, port);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }
}
