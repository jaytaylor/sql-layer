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

package com.akiban.direct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.akiban.sql.embedded.JDBCConnection;

public class DirectContextImpl implements DirectContext {
    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";

    private class ConnectionHolder {
        private Connection connection;
        private ClassLoader contextClassLoader;
        private DirectObject extent;

        private Connection getConnection() {
            if (connection == null) {
                try {
                    connection = DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            return connection;
        }

        private DirectObject getExtent() {
            if (extent == null) {
                Class<?> cl = classLoader.getExtentClass();
                if (cl != null) {
                    try {
                        extent = (DirectObject) cl.newInstance();
                    } catch (InstantiationException | IllegalAccessException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }
            return extent;
        }

        private void enter() {
            contextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
        }

        private void leave() {
            try {
                if (connection != null) {
                    JDBCConnection c = ((JDBCConnection) connection);
                    if (c.isTransactionActive()) {
                        c.rollbackTransaction();
                    }
                }
            } finally {
                Thread.currentThread().setContextClassLoader(contextClassLoader);
            }
        }
    }

    private final DirectClassLoader classLoader;

    private final ThreadLocal<ConnectionHolder> connectionThreadLocal = new ThreadLocal<ConnectionHolder>() {

        @Override
        protected ConnectionHolder initialValue() {
            return new ConnectionHolder();
        }
    };

    public DirectContextImpl(final DirectClassLoader dcl) {
        this.classLoader = dcl;
    }

    @Override
    public Connection getConnection() {
        return connectionThreadLocal.get().getConnection();
    }

    @Override
    public DirectObject getExtent() {
        return connectionThreadLocal.get().getExtent();
    }

    public Statement createStatement() throws SQLException {
        return getConnection().createStatement();
    }

    public void enter() {
        connectionThreadLocal.get().enter();
        Direct.enter(this);
    }

    public void leave() {
        connectionThreadLocal.get().leave();
        Direct.leave();
    }

    public DirectClassLoader getClassLoader() {
        return classLoader;
    }
}
