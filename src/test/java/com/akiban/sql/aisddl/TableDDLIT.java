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

package com.akiban.sql.aisddl;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.UserTable;
import com.akiban.server.api.DDLFunctions;
import com.akiban.sql.pg.PostgresServerITBase;

public class TableDDLIT extends PostgresServerITBase {

    @Test
    public void testCreateSimple() throws Exception {
        String sqlCreate = "CREATE TABLE test.T1 (c1 integer not null primary key)";
        connection.createStatement().execute(sqlCreate);
        
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getUserTable ("test", "t1"));
        
        String sqlDrop = "DROP TABLE test.t1";
        connection.createStatement().execute(sqlDrop);

        ais = ddlServer().getAIS(session());
        assertNull (ais.getUserTable("test", "t1"));
    }
    
    @Test 
    public void testCreateIndexes() throws Exception {
        String sql = "CREATE TABLE test.t1 (c1 integer not null primary key, " + 
            "c2 integer not null, " +
            "constraint c2 unique (c2))";
        connection.createStatement().execute(sql);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        
        UserTable table = ais.getUserTable("test", "t1");
        assertNotNull (table);
        
        assertNotNull (table.getPrimaryKey());
        assertEquals ("PRIMARY", table.getPrimaryKey().getIndex().getIndexName().getName());
        
        assertEquals (2, table.getIndexes().size());
        assertNotNull (table.getIndex("PRIMARY"));
        assertNotNull (table.getIndex("c2"));
    }
    
    @Test
    public void testCreateJoin() throws Exception {
        String sql1 = "CREATE TABLE test.t1 (c1 integer not null primary key)";
        String sql2 = "CREATE TABLE test.t2 (c1 integer not null primary key, " +
            "c2 integer not null, grouping foreign key (c2) references test.t1)";
        
        connection.createStatement().execute(sql1);
        connection.createStatement().execute(sql2);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        

        UserTable table = ais.getUserTable("test", "t2");
        assertNotNull (table);
        assertEquals (1, ais.getJoins().size());
        assertNotNull (table.getParentJoin());
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
}
