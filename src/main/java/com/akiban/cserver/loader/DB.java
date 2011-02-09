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

package com.akiban.cserver.loader;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.akiban.util.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.util.Command;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DB
{
    public DB(String dbHost,
              int dbPort,
              String dbUser,
              String dbPassword)
            throws ClassNotFoundException, SQLException
    {
        this(dbHost, dbPort, dbUser, dbPassword, null);
    }

    public DB(String dbHost,
              int dbPort,
              String dbUser,
              String dbPassword,
              String schema) throws ClassNotFoundException, SQLException
    {
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.dbUser = dbUser;
        this.dbPassword = dbPassword;
        Class.forName("com.mysql.jdbc.Driver");
        dbURL = schema == null ? String.format("jdbc:mysql://%s:%s", dbHost,
                                               dbPort) : String.format("jdbc:mysql://%s:%s/%s", dbHost,
                                                                       dbPort, schema);
    }

    public void spawn(String sql, Logger logger)
    {
        Command command =
                dbPassword == null
                ? Command.logOutput(logger,
                                    LogLevel.INFO,
                                    String.format("%s/bin/mysql", mysqlInstallDir()),
                                    "-h",
                                    dbHost,
                                    "-P",
                                    Integer.toString(dbPort),
                                    "-u",
                                    dbUser)
                : Command.logOutput(logger,
                        LogLevel.INFO,
                                    String.format("%s/bin/mysql", mysqlInstallDir()),
                                    "-h",
                                    dbHost,
                                    "-P",
                                    Integer.toString(dbPort),
                                    "-u",
                                    dbUser,
                                    "-p" + dbPassword);
        try {
            int status = command.run(sql);
            if (status != 0) {
                throw new BulkLoader.DBSpawnFailedException(sql, status, null);
            }
        } catch (IOException e) {
            throw new BulkLoader.DBSpawnFailedException(sql, null, e);
        } catch (Command.Exception e) {
            throw new BulkLoader.DBSpawnFailedException(sql, null, e);
        }
    }

    private String mysqlInstallDir()
    {
        String mysqlInstallDir = System.getProperty(MYSQL_INSTALL_DIR);
        if (mysqlInstallDir == null) {
            throw new BulkLoader.RuntimeException(String.format(
                    "System property %s must be defined.", MYSQL_INSTALL_DIR));
        }
        return mysqlInstallDir;
    }

    private static final String MYSQL_INSTALL_DIR = "mysql.install.dir";

    private final String dbHost;
    private final int dbPort;
    private final String dbUser;
    private final String dbPassword;
    private final String dbURL;

    public class Connection
    {
        public void close() throws SQLException
        {
            connection.close();
        }

        Connection() throws SQLException
        {
            connection = DriverManager.getConnection(dbURL, dbUser, dbPassword);
        }

        private final java.sql.Connection connection;

        public abstract class Query
        {
            public Query(String template, Object... args) throws SQLException
            {
                sql = String.format(template, args);
            }

            public void execute() throws Exception
            {
                // Use TYPE_FORWARD_ONLY, CONCUR_READ_ONLY and
                // setFetchSize(MIN_VALUE) so that result set is streamed.
                // Otherwise, the driver will try to pull the entire result set
                // into memory.
                Statement stmt = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                try {
                    stmt.setFetchSize(Integer.MIN_VALUE);
                    ResultSet resultSet = stmt.executeQuery(sql);
                    try {
                        while (resultSet.next()) {
                            handleRow(resultSet);
                        }
                    } finally {
                        resultSet.close();
                    }
                } finally {
                    stmt.close();
                }
            }

            protected abstract void handleRow(ResultSet resultSet)
                    throws Exception;

            private String sql;
        }

        public class Update
        {
            public Update(String template, Object... args) throws SQLException
            {
                sql = String.format(template, args);
            }

            public int execute() throws SQLException
            {
                Statement stmt = connection.createStatement();
                int updateCount;
                try {
                    updateCount = stmt.executeUpdate(sql);
                } finally {
                    stmt.close();
                }
                return updateCount;
            }

            private String sql;
        }

        public class DDL
        {
            public DDL(String template, Object... args) throws SQLException
            {
                sql = String.format(template, args);
            }

            public void execute() throws SQLException
            {
                Statement stmt = connection.createStatement();
                try {
                    stmt.execute(sql);
                } finally {
                    stmt.close();
                }
            }

            private String sql;
        }
    }
}
