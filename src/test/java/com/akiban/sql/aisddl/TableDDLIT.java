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

    private static final String DROP_T1 = "DROP TABLE test.t1";
    
    @Test
    public void testCreateSimple() throws Exception {
        String sqlCreate = "CREATE TABLE test.T1 (c1 integer not null primary key)";
        connection.createStatement().execute(sqlCreate);
        
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getUserTable ("test", "t1"));
        
        connection.createStatement().execute(DROP_T1);

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

        connection.createStatement().execute(DROP_T1);
        
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
    
    
    public void testCreateComplex() throws Exception {
        String sql1 = "CREATE TABLE t1 ( col1 INTEGER NOT NULL, col2 INTEGER, col3 smallint NOT NULL, "+ 
                    "col4 smallint, col5 bigint NOT NULL, col6 bigint, " +
                    "col10 CHAR(1) NOT NULL, col11 CHAR(1), col12 VARCHAR(1) NOT NULL, col13 VARCHAR(1), " +
                    " col14 LONG VARCHAR NOT NULL, col15 LONG VARCHAR, " +
                    "col20 FLOAT NOT NULL,  col21 FLOAT, col22 REAL NOT NULL, col23 REAL, col24 DOUBLE NOT NULL," +
                    "col25 DOUBLE," + 
                    "col30 DATE NOT NULL, col31 DATE, col32 TIME NOT NULL, col33 time, col34 timestamp NOT NULL, " +
                    "col35 timestamp, "+
                    "col40 CLOB NOT NULL, col41 CLOB, col42 BLOB NOT NULL, col43 BLOB,"+
                    "col50 DECIMAL NOT NULL, col51 DECIMAL, col52 DECIMAL (1) NOT NULL, col53 DECIMAL (1), "+
                    "col54 DECIMAL (10) NOT NULL, col55 DECIMAL (10), col56 DECIMAL (1,1) NOT NULL, " +
                    "col57 DECIMAL (1,1), col58 DECIMAL (10,1) NOT NULL, col59 DECIMAL (10,1), " +
                    "col60 DECIMAL (10,10) NOT NULL, col61 DECIMAL (10,10), col62 DECIMAL (30,10) NOT NULL," +
                    "col63 DECIMAL (30,10),"+
                    "col70 NUMERIC NOT NULL, col71 NUMERIC)";
        connection.setCatalog("test");
        connection.createStatement().execute(sql1);
        UserTable table = ddlServer().getAIS(session()).getUserTable("test", "t1");
        
        assertNotNull (table);
        
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
}
