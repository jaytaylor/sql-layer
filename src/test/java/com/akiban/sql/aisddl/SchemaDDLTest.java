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

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNotNull;
import org.junit.Test;
import org.junit.Before;

import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.server.api.DDLFunctions;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.server.error.DuplicateSchemaException;
import com.akiban.server.error.DropSchemaNotAllowedException;


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
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt);
    }
    
    @Test (expected=DuplicateSchemaException.class)
    public void createSchemaUsed() throws Exception
    {
        String sql = "CREATE SCHEMA S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt);
    }
    
    @Test
    public void dropSchemaEmpty() throws Exception 
    {
        String sql = "DROP SCHEMA EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        DDLFunctions ddlFunctions = new TableDDLTest.DDLFunctionsMock(ais);
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt);
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
        
        SchemaDDL.dropSchema(ddlFunctions, null, (DropSchemaNode)stmt);
    }

    protected SQLParser parser;

    private AkibanInformationSchema factory () throws Exception
    {
        AISBuilder builder = new AISBuilder();
        builder.userTable("s", "t");
        builder.column ("s", "t", "c1", 0, "int", null, null, false, false, null, null);
        builder.index("s", "t", "PRIMARY", true, Index.PRIMARY_KEY_CONSTRAINT);
        builder.indexColumn("s", "t", "PRIMARY", "c1", 0, true, 0);
        builder.basicSchemaIsComplete();
        return builder.akibanInformationSchema();
    }
}
