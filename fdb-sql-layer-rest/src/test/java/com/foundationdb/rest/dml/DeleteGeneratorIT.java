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
package com.foundationdb.rest.dml;

import static org.junit.Assert.assertEquals;

import com.foundationdb.server.types.service.TypesRegistryService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.server.explain.ExplainContext;
import com.foundationdb.server.explain.format.DefaultFormatter;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.test.it.ITBase;

public class DeleteGeneratorIT extends ITBase {

    private DeleteGenerator deleteGenerator; 
    public static final String SCHEMA = "test";

    @Before
    public void start() {
        Session session = this.session();
        this.txnService().beginTransaction(session);
    }
    
    @After
    public void commit() {
        this.txnService().commitTransaction(this.session());
    }

    @Test
    public void testCDelete() {
        createTable(SCHEMA, "c",
                "cid INT PRIMARY KEY NOT NULL",
                "name VARCHAR(32)");    

        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), cid = $1)");
    }

    @Test 
    public void testPKNotFirst() {
        createTable (SCHEMA, "c",
                "name varchar(32) not null",
                "address varchar(64) not null",
                "cid int not null primary key");
        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), cid = $1)");
    }

    @Test
    public void testPKMultiColumn() {
        createTable(SCHEMA, "o",
                "cid int not null",
                "oid int not null",
                "items int not null",
                "primary key (cid, oid)");
        TableName table = new TableName (SCHEMA, "o");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals(
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(o.PRIMARY) -> o)\n"+
                "      IndexScan_Default(Index(o.PRIMARY), cid = $1, oid = $2)");
    }
    
    @Test
    public void testNoPK() {
        createTable (SCHEMA, "c",
                "name varchar(32) not null",
                "address varchar(64) not null",
                "cid int not null");
        TableName table = new TableName (SCHEMA, "c");
        this.deleteGenerator = new DeleteGenerator (this.ais());
        deleteGenerator.setTypesRegistry(this.serviceManager().getServiceByClass(TypesRegistryService.class));
        Operator delete = deleteGenerator.create(table);
        assertEquals (
                getExplain(delete, table.getSchemaName()),
                "\n  Delete_Returning()\n"+
                "    GroupLookup_Default(Index(c.PRIMARY) -> c)\n"+
                "      IndexScan_Default(Index(c.PRIMARY), __row_id = $1)");
    }

    protected String getExplain (Operator plannable, String defaultSchemaName) {
        StringBuilder str = new StringBuilder();
        ExplainContext context = new ExplainContext(); // Empty
        DefaultFormatter f = new DefaultFormatter(defaultSchemaName);
        for (String operator : f.format(plannable.getExplainer(context))) {
            str.append("\n  ");
            str.append(operator);
        }
        return str.toString();
    }
}
