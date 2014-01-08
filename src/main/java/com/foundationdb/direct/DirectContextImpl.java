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

package com.foundationdb.direct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import com.foundationdb.sql.embedded.JDBCConnection;

public class DirectContextImpl implements DirectContext {
    public static final String CONNECTION_URL = "jdbc:default:connection";

    private final String space;
    private final DirectContextImpl parent;
    
    static class ConnectionHolder {
        private String space;
        private DirectClassLoader classLoader;
        private Connection connection;
        private ClassLoader contextClassLoader;
        private DirectObject extent;
        private boolean connectionOpened;

        public ConnectionHolder(String space, DirectClassLoader classLoader) {
            this.space = space;
            this.classLoader = classLoader;
        }

        public Connection getConnection() {
            if (connection == null) {
                try {
                    connection = DriverManager.getConnection(CONNECTION_URL, space, "");
                    connectionOpened = true;
                } catch (SQLException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
            return connection;
        }
        
        public void setConnection(final JDBCConnection connection) {
            this.connection = connection;
        }

        public DirectObject getExtent() {
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

        public void enter() {
            contextClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
        }

        public void leave() {
            try {
                if (connection != null) {
                    try {
                        if (connectionOpened) {
                            connection.close();
                        }
                        connection = null;
                        connectionOpened = false;
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

    static class ConnectionThreadLocal extends ThreadLocal<ConnectionHolder> {
        private String space;
        private DirectClassLoader classLoader;
        
        public ConnectionThreadLocal(String space, DirectClassLoader classLoader) {
            this.space = space;
            this.classLoader = classLoader;
        }

        @Override
        protected ConnectionHolder initialValue() {
            return new ConnectionHolder(space, classLoader);
        }
    }

    private final ThreadLocal<ConnectionHolder> connectionThreadLocal;

    public DirectContextImpl(final String space, final DirectClassLoader dcl,
                             final DirectContextImpl parent) {
        this.space = space;
        this.classLoader = dcl;
        this.parent = parent;
        this.connectionThreadLocal = new ConnectionThreadLocal(space, classLoader);
    }

    @Override
    public Connection getConnection() {
        return connectionThreadLocal.get().getConnection();
    }
    
    public void setConnection(JDBCConnection connection) {
        connectionThreadLocal.get().setConnection(connection);
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

    public DirectContextImpl getParent() {
        return parent;
    }
}
