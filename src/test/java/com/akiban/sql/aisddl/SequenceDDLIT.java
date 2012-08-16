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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.ResultSet;

import org.junit.Test;
import org.postgresql.util.PSQLException;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.ErrorCode;
import com.akiban.sql.pg.PostgresServerITBase;
import com.ibm.icu.text.MessageFormat;


public class SequenceDDLIT extends PostgresServerITBase {

    
    @Test (expected=PSQLException.class)
    public void dropSequenceFail() throws Exception{
        String sql = "DROP SEQUENCE not_here";
        getConnection().createStatement().execute(sql);
    }
    
    @Test
    public void createSequence () throws Exception {
        String sql = "CREATE SEQUENCE new_sequence";
        getConnection().createStatement().execute(sql);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getSequence(new TableName ("test", "new_sequence")));
        
        sql = "DROP SEQUENCE new_sequence restrict";
        getConnection().createStatement().execute(sql);
        ais = ddlServer().getAIS(session());
        assertEquals (0, ais.getSequences().size());
    }

    @Test 
    public void duplicateSequence() throws Exception {
        String sql = "CREATE SEQUENCE test.new_sequence";
        getConnection().createStatement().execute(sql);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getSequence(new TableName ("test", "new_sequence")));

        try {
            getConnection().createStatement().execute(sql);
            fail ("Duplicate Sequence not checked");
        } catch (PSQLException ex) {
            assertEquals("ERROR: Sequence `test`.`new_sequence` already exists", ex.getMessage());
        }

        sql = "DROP SEQUENCE test.new_sequence restrict";
        getConnection().createStatement().execute(sql);
        ais = ddlServer().getAIS(session());
        assertEquals (0, ais.getSequences().size());

    }
    
    @Test (expected=PSQLException.class)
    public void doubleDropSequence() throws Exception {
        String sql = "CREATE SEQUENCE test.new_sequence";
        getConnection().createStatement().execute(sql);
        AkibanInformationSchema ais = ddlServer().getAIS(session());
        assertNotNull (ais.getSequence(new TableName ("test", "new_sequence")));

        sql = "DROP SEQUENCE test.new_sequence restrict";
        getConnection().createStatement().execute(sql);
        ais = ddlServer().getAIS(session());
        assertEquals (0, ais.getSequences().size());

        // fails for the second one due to non-existence of the sequence. 
        getConnection().createStatement().execute(sql);
    }
    
    @Test 
    public void dropSequenceExists() throws Exception {
        String sql = "DROP SEQUENCE IF EXISTS test.not_exists RESTRICT";
        Statement stmt = getConnection().createStatement();
        stmt.execute(sql);
        SQLWarning warn = stmt.getWarnings();
        assertNotNull(warn);
        assertEquals(warn.getMessage(), MessageFormat.format(ErrorCode.NO_SUCH_SEQUENCE.getMessage(), "test", "not_exists"));
    }

    @Test
    public void testSequenceValues() throws Exception {
        String sql = "DROP SEQUENCE IF EXISTS test.new_sequence RESTRICT";
        Statement stmt = getConnection().createStatement();
        stmt.execute(sql);
        sql = "CREATE SEQUENCE test.new_sequence START WITH 5 INCREMENT BY 5";
        getConnection().createStatement().execute(sql);
        sql = "SELECT NEXT VALUE FOR test.new_sequence";
        stmt = getConnection().createStatement();
        stmt.execute(sql);
        ResultSet rs = stmt.getResultSet();
        long nextValue = rs.next() ? rs.getLong(1) : 0;
        assertEquals(nextValue, 5);
        sql = "SELECT CURRENT VALUE FOR test.new_sequence";
        stmt = getConnection().createStatement();
        stmt.execute(sql);
        rs = stmt.getResultSet();
        long currentValue = rs.next() ? rs.getLong(1) : 0;
        assertEquals(currentValue, nextValue);
    }
    
    protected DDLFunctions ddlServer() {
        return serviceManager().getDXL().ddlFunctions();
    }
    
 

}
