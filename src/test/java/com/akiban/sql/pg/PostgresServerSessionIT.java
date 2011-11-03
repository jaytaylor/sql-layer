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
package com.akiban.sql.pg;

import static junit.framework.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.api.DDLFunctions;
import org.postgresql.util.PSQLException;

public class PostgresServerSessionIT extends PostgresServerFilesITBase {

    @Before
    public void createSimpleSchema() throws SQLException {
        String sqlCreate = "CREATE TABLE test.T1 (c1 integer not null primary key)";
        connection.createStatement().execute(sqlCreate);
    }
    
    @Test
    public void createNewTable() throws SQLException {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        connection.createStatement().execute(create);
        
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getUserTable("user", "t1"));
        assertNotNull (ais.getUserTable("test", "t1"));

    }

    @Test 
    public void goodUseSchema() throws SQLException {
        String use = "SET SCHEMA test";
        connection.createStatement().execute(use);
        
        String create = "CREATE TABLE t2 (c2 integer not null primary key)";
        connection.createStatement().execute(create);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getUserTable("test", "t2"));
        assertNotNull (ais.getUserTable("test", "t1"));
    }
    
    @Test (expected=PSQLException.class)
    public void badUseSchema() throws SQLException {
        String use = "SET SCHEMA BAD";
        connection.createStatement().execute(use);
    }
    
    @Test 
    public void useUserSchema() throws SQLException {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        connection.createStatement().execute(create);
        
        connection.createStatement().execute("SET SCHEMA TEST");
        
        connection.createStatement().execute("SET SCHEMA USER");
        
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }

}
