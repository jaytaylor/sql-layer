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

package com.akiban.sql.pg;

import static junit.framework.Assert.assertNotNull;

import java.sql.SQLException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.api.DDLFunctions;
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
        assertNotNull (ais.getUserTable("fake", "t1"));
        assertNotNull (ais.getUserTable(SCHEMA_NAME, "t1"));

    }

    @Test 
    public void goodUseSchema() throws Exception {
        String use = "SET SCHEMA fake";
        getConnection().createStatement().execute(use);
        
        String create = "CREATE TABLE t2 (c2 integer not null primary key)";
        getConnection().createStatement().execute(create);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getUserTable("fake", "t2"));
        assertNotNull (ais.getUserTable("fake", "t1"));
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
