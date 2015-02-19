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

import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.sql.pg.PostgresServer;
import com.foundationdb.sql.pg.PostgresServerManager;
import com.foundationdb.sql.pg.PostgresService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.fail;

public class PostgresMTBase extends MTBase
{
    private static final Logger LOG = LoggerFactory.getLogger(PostgresMTBase.class);

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:fdbsql://%s:%d/"+SCHEMA_NAME;
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
        String url = String.format(CONNECTION_URL, server.getHost(), port);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    protected static abstract class QueryThread extends Thread
    {
        protected final String schema;
        protected final Connection conn;
        private Statement s;

        public QueryThread(String name, String schema, Connection conn) {
            super(name);
            this.schema = schema;
            this.conn = conn;
        }

        protected abstract int getLoopCount();
        protected abstract String[] getQueries();

        @Override
        public void run() {
            int loopCount = getLoopCount();
            String[] queries = getQueries();
            try {
                for(int i = 0; i < loopCount; ++i) {
                    for(String q : queries) {
                        execQuery(q);
                        delay();
                    }
                }
            } finally {
                try {
                    conn.close();
                } catch(SQLException e) {
                    // Ignore
                }
            }
        }

        private void delay() {
            try {
                Thread.sleep(10);
            } catch(InterruptedException e) {
                // Ignore
            }
        }

        private void execQuery(String query) {
            for(;;) {
                try {
                    if(s == null) {
                        s = conn.createStatement();
                    }
                    s.execute(query);
                    break;
                } catch(SQLException e) {
                    ErrorCode code = ErrorCode.valueOfCode(e.getSQLState());
                    if(code == ErrorCode.STALE_STATEMENT) {
                        // retry with new statement
                        try {
                            s.close();
                        } catch(SQLException e1) {
                            // Ignore
                        }
                        s = null;
                    } else if(code.isRollbackClass()) {
                        try {
                            if(!conn.getAutoCommit()) {
                                conn.rollback();
                            }
                        } catch(SQLException e1) {
                            throw new RuntimeException("Error rolling back", e1);
                        }
                        // retry after slight delay
                        delay();
                    } else {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
    }
}
