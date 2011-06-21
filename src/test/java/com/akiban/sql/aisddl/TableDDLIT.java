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

import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
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
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
}
