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

import com.akiban.server.error.ErrorCode;

import org.junit.Before;
import org.junit.Test;
import static junit.framework.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class JMXCancelationIT extends PostgresServerITBase
{
    private static final int SERVER_JMX_PORT = 8082;
    private static final String SERVER_ADDRESS = "localhost";
    private static final int N = 1000;

    @Before
    public void loadDB() throws Exception {
        Statement statement = getConnection().createStatement();
        statement.execute("CREATE TABLE t(id INTEGER NOT NULL PRIMARY KEY)");
        for (int id = 0; id < N; id++) {
            statement.execute(String.format("INSERT INTO t VALUES(%s)", id));
        }
        statement.close();
    }

    @Test
    public void testCancel() throws Exception {
        test("cancelQuery", false);
    }

    @Test
    public void testKillConnection() throws Exception {
        test("killConnection", true);
    }

    private void test(String method, boolean closedOK) throws Exception {
        JMXInterpreter jmx = null;
        try {
            jmx = new JMXInterpreter(false);

            Integer[] sessions = (Integer[])
                jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                                 "com.akiban:type=PostgresServer",
                                 "CurrentSessions", null, "get");
            List<Integer> before = Arrays.asList(sessions);

            Thread queryThread = startQueryThread(closedOK);
            Thread.sleep(250);

            sessions = (Integer[])
                jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                                 "com.akiban:type=PostgresServer",
                                 "CurrentSessions", null, "get");
            List<Integer> after = Arrays.asList(sessions);
            after = new ArrayList<Integer>(after);
            after.removeAll(before);

            assertEquals(1, after.size());
            Integer session = after.get(0);

            jmx.makeBeanCall(SERVER_ADDRESS, SERVER_JMX_PORT,
                             "com.akiban:type=PostgresServer",
                             method, new Object[] { session }, "method");

            queryThread.join();
        }
        finally {
            if (jmx != null) {
                jmx.close();
            }
        }
    }

    private Thread startQueryThread(final boolean closedOK) throws Exception {
        Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    Connection connection = null;
                    Statement statement = null;
                    try {
                        connection = openConnection();
                        statement = connection.createStatement();
                        statement.execute("SELECT COUNT(*) FROM t t1, t t2, t t3");
                        fail("Query should not complete.");
                    }
                    catch (SQLException ex) {
                        String sqlState = ex.getSQLState();
                        // Kill case can see either cancel or connection close.
                        if (!closedOK || !"08006".equals(sqlState))
                            assertEquals(ErrorCode.QUERY_CANCELED.getFormattedValue(), sqlState);
                    }
                    catch (Exception ex) {
                        fail(ex.toString());
                    }
                    finally {
                        try {
                            if (statement != null)
                                statement.close();
                            closeConnection(connection);
                        }
                        catch (Exception ex) {
                        }
                    }
                }
            });
        thread.setDaemon(false);
        thread.start();
        return thread;
    }

}
