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

    public static final String SCHEMA_NAME = "test";
    public static final String DRIVER_NAME = "org.postgresql.Driver";
    public static final String CONNECTION_URL = "jdbc:postgresql://localhost:%d/"+SCHEMA_NAME;
    public static final String USER_NAME = "auser";
    public static final String USER_PASSWORD = "apassword";

    protected Connection openConnection() throws Exception {
        int port = getPostgresService().getPort();
        if (port <= 0) {
            throw new Exception("akserver.postgres.port is not set.");
        }
        String url = String.format(CONNECTION_URL, port);
        Class.forName(DRIVER_NAME);
        return DriverManager.getConnection(url, USER_NAME, USER_PASSWORD);
    }

    protected static void closeConnection(Connection connection) throws Exception {
        connection.close();
    }

    protected PostgresService getPostgresService() {
        return serviceManager().getServiceByClass(PostgresService.class);
    }

    protected PostgresServer server() {
        return getPostgresService().getServer();
    }

    // One element connection pool.
    private static Connection connection = null;

    protected Connection getConnection() throws Exception {
        if (connection == null) {
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
        return connection;
    }

    public static void forgetConnection() throws Exception {
        if (connection != null) {
            closeConnection(connection);
            connection = null;
        }
    }

    protected PostgresServerITBase() {
    }

}
