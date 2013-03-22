
package com.akiban.direct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.akiban.sql.embedded.JDBCConnection;

public class DirectContextImpl implements DirectContext {
    public static final String CONNECTION_URL = "jdbc:default:connection";

    private final String space;
    
    private class ConnectionHolder {
        private Connection connection;
        private ClassLoader contextClassLoader;
        private DirectObject extent;

        private Connection getConnection() {
            if (connection == null) {
                try {
                    connection = DriverManager.getConnection(CONNECTION_URL, space, "");
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
                    try {
                        connection.close();
                        connection = null;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
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

    public DirectContextImpl(final String space, final DirectClassLoader dcl) {
        this.space = space;
        this.classLoader = dcl;
    }

    @Override
    public Connection getConnection() {
        return connectionThreadLocal.get().getConnection();
    }

    @Override
    public DirectObject getExtent() {
        classLoader.ensureGenerated();
        return connectionThreadLocal.get().getExtent();
    }

    public Statement createStatement() throws SQLException {
        return getConnection().createStatement();
    }

    public void enter() {
        connectionThreadLocal.get().enter();
    }

    public void leave() {
        connectionThreadLocal.get().leave();
    }

    public DirectClassLoader getClassLoader() {
        return classLoader;
    }
}
