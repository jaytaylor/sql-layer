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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

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
    public void testSelect() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        long querys = getMonitorService().getCount(StatementTypes.SELECT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT name FROM c WHERE cid = 1")) {
            assertTrue("has first row", rs.next());
            assertEquals("result value", "Smith", rs.getString(1));
            assertFalse("has more rows", rs.next());
        }
        
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long requerys = getMonitorService().getCount(StatementTypes.SELECT);
        
        assertEquals (1, recount-count);
        assertEquals (1, requerys-querys);
    }

    @Test
    public void testDDL() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        long ddl = getMonitorService().getCount(StatementTypes.DDL_STMT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
             stmt.executeUpdate("CREATE TABLE i (iid int primary key not null, count int not null)");
        }
        
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long reddl =  getMonitorService().getCount(StatementTypes.DDL_STMT);

        
        assertEquals (1, recount-count);
        assertEquals (1, reddl-ddl);
    }

    @Test
    public void testDML() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        long ddl = getMonitorService().getCount(StatementTypes.DML_STMT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
             stmt.executeUpdate("INSERT INTO c (cid, name) values (3, 'franks')");
        }
        
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long reddl =  getMonitorService().getCount(StatementTypes.DML_STMT);

        
        assertEquals (1, recount-count);
        assertEquals (1, reddl-ddl);
    }

    @Test
    public void testCall() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        long ddl = getMonitorService().getCount(StatementTypes.DDL_STMT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
             stmt.executeUpdate("CREATE PROCEDURE test_clean(OUT total INT) AS $$\n"+
                     "function fun(total) {\n" + 
                     "var conn = java.sql.DriverManager.getConnection('jdbc:default:connection');\n" +
                     "var stmt = conn.createStatement();\n"+
                     "var rs = stmt.executeQuery('SELECT sum(cid) FROM c');\n"+
                     "rs.next();\n"+
                     "total[0] = rs.getInt(1);\n"+
                     "rs.close();\n"+
                     "stmt.close();\n"+
                     "conn.close();\n"+
                     "}\n"+
                     "$$ LANGUAGE javascript PARAMETER STYLE java EXTERNAL NAME 'fun'");
        }
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long reddl =  getMonitorService().getCount(StatementTypes.DDL_STMT);
        assertEquals (1, recount-count);
        assertEquals (1, reddl-ddl);

        long calls = getMonitorService().getCount(StatementTypes.CALL_STMT);
        long querys = getMonitorService().getCount(StatementTypes.SELECT);
        try (Connection conn = getConnection();
                CallableStatement stmt = conn.prepareCall("CALL test_clean(?)")) {
            stmt.registerOutParameter(1, Types.INTEGER);
            stmt.execute();
            assertEquals(3, stmt.getInt(1));
        }
        long newCount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long recalls = getMonitorService().getCount(StatementTypes.CALL_STMT);
        long requerys = getMonitorService().getCount(StatementTypes.SELECT);
        
        assertEquals(2, newCount-recount);
        assertEquals(1, recalls-calls);
        assertEquals(1, requerys-querys);
 
    }
    
    @Test
    public void testCallRS() throws Exception {
        long count = getMonitorService().getCount(StatementTypes.STATEMENT);
        long ddl = getMonitorService().getCount(StatementTypes.DDL_STMT);
        try (Connection conn = getConnection();
             Statement stmt = conn.createStatement()) {
             stmt.executeUpdate("CREATE PROCEDURE test_rs() DYNAMIC RESULT SETS 1 AS $$\n"+
                     "function fun(rs) {\n"+ 
                     "var conn = java.sql.DriverManager.getConnection('jdbc:default:connection');\n"+
                     "var stmt = conn.createStatement();\n"+
                     "rs[0] = stmt.executeQuery('SELECT sum(cid) FROM c');\n"+
                     "}\n"+
                     "$$ LANGUAGE javascript PARAMETER STYLE java EXTERNAL NAME 'fun'");
        }
        long recount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long reddl =  getMonitorService().getCount(StatementTypes.DDL_STMT);
        assertEquals (1, recount-count);
        assertEquals (1, reddl-ddl);
        long calls = getMonitorService().getCount(StatementTypes.CALL_STMT);
        long querys = getMonitorService().getCount(StatementTypes.SELECT);
        try (Connection conn = getConnection();
                CallableStatement stmt = conn.prepareCall("CALL test_rs()");
                ResultSet rs = stmt.executeQuery()) {
            assertNotNull (rs);
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
            assertFalse("has more rows", rs.next());
            
        }
        long newCount = getMonitorService().getCount(StatementTypes.STATEMENT);
        long recalls = getMonitorService().getCount(StatementTypes.CALL_STMT);
        long requerys = getMonitorService().getCount(StatementTypes.SELECT);
        
        assertEquals(2, newCount-recount);
        assertEquals(1, recalls-calls);
        assertEquals(1, requerys-querys);

    
    }
   
}
