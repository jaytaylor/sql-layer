/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
import com.akiban.server.error.NoSuchSchemaException;


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
    
    @Test (expected=NoSuchSchemaException.class)
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
