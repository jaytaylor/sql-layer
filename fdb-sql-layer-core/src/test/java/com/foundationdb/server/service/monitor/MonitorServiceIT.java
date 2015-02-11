/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
package com.foundationdb.server.service.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;

import com.foundationdb.server.service.is.ServerSchemaTablesService;
import com.foundationdb.server.service.is.ServerSchemaTablesServiceImpl;
import com.foundationdb.server.service.metrics.FDBMetricsService;
import com.foundationdb.server.service.metrics.MetricsService;
import com.foundationdb.server.service.monitor.SessionMonitor.StatementTypes;
import com.foundationdb.server.service.servicemanager.GuicedServiceManager;
import com.foundationdb.server.test.it.ITBase;
import com.foundationdb.sql.embedded.EmbeddedJDBCService;
import com.foundationdb.sql.embedded.EmbeddedJDBCServiceImpl;

public class MonitorServiceIT extends ITBase {

    @Override
    protected GuicedServiceManager.BindingsConfigurationProvider serviceBindingsProvider() {
        return super.serviceBindingsProvider()
                    .bindAndRequire(EmbeddedJDBCService.class, EmbeddedJDBCServiceImpl.class)
                    .bindAndRequire(MonitorService.class, MonitorServiceImpl.class);
    }

    public static final String SCHEMA_NAME = "test";
    public static final String CONNECTION_URL = "jdbc:default:connection";

    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(CONNECTION_URL, SCHEMA_NAME, "");
    }
    
    public MonitorService getMonitorService() {
        return (MonitorService)serviceManager().getServiceByClass(MonitorService.class);
    }
    
    @Before
    public void loadDB() throws Exception {
        int cid = createTable(SCHEMA_NAME, "c",
                              "cid int primary key not null",
                              "name varchar(16) not null");
        int oid = createTable(SCHEMA_NAME, "o",
                              "oid int primary key not null",
                              "cid int not null",
                              "grouping foreign key(cid) references c(cid)",
                              "order_date date not null");
        writeRow(cid, 1, "Smith");
        writeRow(oid, 101, 1, 2012 * 512 + 1 * 32 + 31);
        writeRow(oid, 102, 1, 2012 * 512 + 2 * 32 + 1);
        writeRow(cid, 2, "Jones");
        writeRow(oid, 201, 2, 2012 * 512 + 4 * 32 + 1);
    }
    
    @Test
    public void testSimple() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM c WHERE cid = 1")) {
            assertTrue("has first row", rs.next());
            assertEquals("result value", "Smith", rs.getString(1));
            assertFalse("has more rows", rs.next());
        }
        
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        
        assertEquals (1, recount-count);
    }


}
