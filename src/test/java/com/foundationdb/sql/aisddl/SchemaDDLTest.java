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

package com.foundationdb.sql.aisddl;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNotNull;
import org.junit.Test;
import org.junit.Before;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.TestAISBuilder;
import com.foundationdb.server.api.DDLFunctions;
import com.foundationdb.sql.parser.SQLParser;
import com.foundationdb.sql.parser.StatementNode;
import com.foundationdb.sql.parser.CreateSchemaNode;
import com.foundationdb.sql.parser.DropSchemaNode;
import com.foundationdb.server.error.DuplicateSchemaException;
import com.foundationdb.server.error.DropSchemaNotAllowedException;
import com.foundationdb.server.error.NoSuchSchemaException;
import com.foundationdb.server.types.service.TestTypesRegistry;
import com.foundationdb.server.types.service.TypesRegistry;

public class SchemaDDLTest {

    @Before
    public void before() throws Exception {
        parser = new SQLParser();
    }

    @Test
    public void createSchemaEmpty () throws Exception
    {
        String sql = "CREATE SCHEMA EMPTY";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
    
    @Test (expected=DuplicateSchemaException.class)
    public void createSchemaUsed() throws Exception
    {
        String sql = "CREATE SCHEMA S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
    
      
    @Test //(expected=DuplicateSchemaException.class)
    public void createNonDuplicateSchemaWithIfNotExist() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS SS";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }
        
    @Test
    public void createDuplicateSchemaWithIfNotExist() throws Exception
    {
        String sql = "CREATE SCHEMA IF NOT EXISTS S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt, null);
    }

    @Test (expected=NoSuchSchemaException.class)
    public void dropSchemaEmpty() throws Exception 
    {
        String sql = "DROP SCHEMA EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    @Test
    public void dropNonExistingSchemaWithIfExists() throws Exception
    {
        String sql = "DROP SCHEMA IF EXISTS AA";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode node = parser.parseStatement(sql);
        assertTrue(node instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)node, null);
    }

    @Test
    public void dropSchemaEmptyIfExists() throws Exception 
    {
        String sql = "DROP SCHEMA IF EXISTS EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    @Test(expected=DropSchemaNotAllowedException.class)
    public void dropSchemaUsed() throws Exception
    {
        String sql = "DROP SCHEMA S RESTRICT";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt, null);
    }

    protected SQLParser parser;

    private AkibanInformationSchema factory () throws Exception
    {
        TypesRegistry typesRegistry = TestTypesRegistry.MCOMPAT;
        TestAISBuilder builder = new TestAISBuilder(typesRegistry);
        builder.table("s", "t");
        builder.column ("s", "t", "c1", 0, "MCOMPAT", "int", false);
        builder.pk("s", "t");
        builder.indexColumn("s", "t", "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        return builder.akibanInformationSchema();
    }
}
