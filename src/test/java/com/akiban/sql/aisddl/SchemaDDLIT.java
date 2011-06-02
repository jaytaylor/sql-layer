package com.akiban.sql.aisddl;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;
import org.junit.Test;
import org.junit.Before;

import com.akiban.ais.ddl.SchemaDef;
import com.akiban.ais.ddl.SchemaDefToAis;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.server.SchemaFactory;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.StatementNode;
import com.akiban.sql.parser.CreateSchemaNode;
import com.akiban.sql.parser.DropSchemaNode;
import com.akiban.sql.StandardException;


public class SchemaDDLIT {

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
    
    @Test
    public void createSchemaUsed() throws Exception
    {
        String sql = "CREATE SCHEMA S";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof CreateSchemaNode);
        
        try {
            SchemaDDL.createSchema(ais, null, (CreateSchemaNode)stmt);
            fail();
        } catch (StandardException ex) {
            ; // do nothing, exception expected. 
        }
    }
    
    @Test
    public void dropSchemaEmpty() throws Exception 
    {
        String sql = "DROP SCHEMA EMPTY RESTRICT";
        AkibanInformationSchema ais = new AkibanInformationSchema();
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        SchemaDDL.dropSchema(ais, null, (DropSchemaNode)stmt);
        
    }

    @Test
    public void dropSchemaUsed() throws Exception
    {
        String sql = "DROP SCHEMA S RESTRICT";
        AkibanInformationSchema ais = factory();
        
        assertNotNull(ais.getTable("s", "t"));
        
        StatementNode stmt = parser.parseStatement(sql);
        assertTrue (stmt instanceof DropSchemaNode);
        
        try {
            SchemaDDL.dropSchema(ais, null, (DropSchemaNode)stmt);
            fail();
        } catch (StandardException ex) {
            ; // do nothing, exception expected. 
        }
        
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
