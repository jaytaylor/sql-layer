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

package com.foundationdb.sql.JDBCProxy;

import java.sql.Connection;
import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class ProxyDriverImpl implements java.sql.Driver {

    Driver embeddedJDBCDriver;
    
    public ProxyDriverImpl(Driver embeddedJDBCDriver) {
        this.embeddedJDBCDriver = embeddedJDBCDriver;    
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        return this.embeddedJDBCDriver.connect(url, info);
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return this.embeddedJDBCDriver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return this.embeddedJDBCDriver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return this.embeddedJDBCDriver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return this.embeddedJDBCDriver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return this.embeddedJDBCDriver.jdbcCompliant();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return this.embeddedJDBCDriver.getParentLogger();
    }
}
