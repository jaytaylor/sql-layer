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

package com.foundationdb.sql.pg;

import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.server.api.DDLFunctions;
import org.postgresql.util.PSQLException;

public class PostgresServerSessionIT extends PostgresServerFilesITBase {

    @Before
    public void createSimpleSchema() throws Exception {
        String sqlCreate = "CREATE TABLE fake.T1 (c1 integer not null primary key)";
        getConnection().createStatement().execute(sqlCreate);
    }
    
    @After
    public void dontLeaveConnection() throws Exception {
        // Tests change current schema. Easiest not to reuse.
        forgetConnection();
    }

    @Test
    public void createNewTable() throws Exception {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getTable("fake", "t1"));
        assertNotNull (ais.getTable(SCHEMA_NAME, "t1"));

    }

    @Test 
    public void goodUseSchema() throws Exception {
        String use = "SET SCHEMA fake";
        getConnection().createStatement().execute(use);
        
        String create = "CREATE TABLE t2 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getTable("fake", "t2"));
        assertNotNull (ais.getTable("fake", "t1"));
    }
    
    @Test (expected=PSQLException.class)
    public void badUseSchema() throws Exception {
        String use = "SET SCHEMA BAD";
        getConnection().createStatement().execute(use);
    }
    
    @Test 
    public void useUserSchema() throws Exception {
        String create  = "CREATE TABLE t1 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        create  = "CREATE TABLE auser.t1 (c4 integer not null primary key)";
        getConnection().createStatement().execute(create);
        
        getConnection().createStatement().execute("SET SCHEMA auser");
        
        getConnection().createStatement().execute("SET SCHEMA "+PostgresServerITBase.SCHEMA_NAME);
        
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }

}
