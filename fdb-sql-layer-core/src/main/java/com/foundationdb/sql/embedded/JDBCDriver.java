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

package com.foundationdb.sql.embedded;

import com.foundationdb.sql.server.ServerServiceRequirements;
import com.foundationdb.server.service.monitor.ServerMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

public class JDBCDriver implements Driver, ServerMonitor {
    public static final String URL = "jdbc:default:connection";

    private final ServerServiceRequirements reqs;
    private long startTime;
    private int nconnections;

    private static final Logger logger = LoggerFactory.getLogger(JDBCDriver.class);

    protected JDBCDriver(ServerServiceRequirements reqs) {
        this.reqs = reqs;
    }

    public void register() throws SQLException {
        startTime = System.currentTimeMillis();
        DriverManager.registerDriver(this);
        reqs.monitor().registerServerMonitor(this);
    }

    public void deregister() throws SQLException {
        reqs.monitor().deregisterServerMonitor(this);
        DriverManager.deregisterDriver(this);
    }

    /* Driver */

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!url.equals(URL)) return null;
        nconnections++;
        return new JDBCConnection(reqs, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url.equals(URL);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }


    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public java.util.logging.Logger getParentLogger() 
            throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Uses LOG4J");
    }

    /* ServerMonitor */

    @Override
    public String getServerType() {
        return JDBCConnection.SERVER_TYPE;
    }

    @Override
    public int getLocalPort() {
        return -1;
    }

    @Override
    public String getLocalHost() {
        return null;
    }

    @Override
    public long getStartTimeMillis() {
        return startTime;
    }
    
    @Override
    public int getSessionCount() {
        return nconnections;
    }

}
