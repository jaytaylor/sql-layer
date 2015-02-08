/**
 * Copyright (C) 2009-2014 FoundationDB, LLC
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
package com.foundationdb.server.test.mt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.Test;

public class OverlappingDDLAndDMLMT extends PostgresMTBase {

    
    @Test
    public void statementOverlap() throws SQLException {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);

        
        Connection conn2 = createConnection();
        conn2.setAutoCommit(true);
        
        PreparedStatement p1 = conn1.prepareStatement("SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        
        assertTrue (p1.execute());
    }
    
    @Test
    public void prepareOverlap() throws SQLException {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);
       
        Connection conn2 = createConnection();
        conn2.setAutoCommit(true);
        
        conn1.createStatement().execute("PREPARE q1 as SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        
        try {
        assertTrue(!conn1.createStatement().execute("EXECUTE q1"));
        } catch (SQLException e) {
            assertEquals (e.getMessage(), "ERROR: Unusable prepared statement due to DDL after generation");
            assertEquals (e.getSQLState(), "0A50A");
        }
    }
    
    @Test
    public void prepareOverlapRollback() throws SQLException   {
        Connection conn1 = createConnection(); 
        conn1.setAutoCommit(false);
        
        Connection conn2 = createConnection();
        conn2.setAutoCommit(false);
        
        conn1.createStatement().execute("PREPARE q1 as SELECT * from information_schema.tables");
        conn1.commit();
        
        conn2.createStatement().execute("Create Table t (id int not null primary key, name varchar(32))");
        conn2.rollback();
        
        try {
        assertTrue(!conn1.createStatement().execute("EXECUTE q1"));
        } catch (SQLException e) {
            assertEquals (e.getMessage(), "ERROR: Unusable prepared statement due to DDL after generation");
            assertEquals (e.getSQLState(), "0A50A");
        }
        
    }
}
