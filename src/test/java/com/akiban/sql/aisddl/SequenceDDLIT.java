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

import org.junit.Test;

import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.error.ErrorCode;
import com.akiban.server.error.NoSuchSequenceException;
import com.akiban.sql.parser.SQLParserException;
import com.ibm.icu.text.MessageFormat;
import java.util.Collections;

public class SequenceDDLIT extends AISDDLITBase {

    
    @Test (expected=SQLParserException.class)
    public void dropSequenceFail() throws Exception{
        String sql = "DROP SEQUENCE not_here";
        executeDDL(sql);
    }
    
    @Test
    public void createSequence () throws Exception {
        String sql = "CREATE SEQUENCE new_sequence";
        executeDDL(sql);
        assertNotNull (ais().getSequence(new TableName ("test", "new_sequence")));
        
        sql = "DROP SEQUENCE new_sequence restrict";
        executeDDL(sql);
        assertEquals (0, ais().getSequences().size());
    }

    @Test 
    public void duplicateSequence() throws Exception {
        String sql = "CREATE SEQUENCE test.new_sequence";
        executeDDL(sql);
        assertNotNull (ais().getSequence(new TableName ("test", "new_sequence")));

        try {
            executeDDL(sql);
            fail ("Duplicate Sequence not checked");
        } catch (Exception ex) {
            assertEquals("DUPLICATE_SEQUENCE: Sequence `test`.`new_sequence` already exists", ex.getMessage());
        }

        sql = "DROP SEQUENCE test.new_sequence restrict";
        executeDDL(sql);
        assertEquals (0, ais().getSequences().size());

    }
    
    @Test (expected=NoSuchSequenceException.class)
    public void doubleDropSequence() throws Exception {
        String sql = "CREATE SEQUENCE test.new_sequence";
        executeDDL(sql);
        assertNotNull (ais().getSequence(new TableName ("test", "new_sequence")));

        sql = "DROP SEQUENCE test.new_sequence restrict";
        executeDDL(sql);
        assertEquals (0, ais().getSequences().size());

        // fails for the second one due to non-existence of the sequence. 
        executeDDL(sql);
    }
    
    @Test 
    public void dropSequenceExists() throws Exception {
        String sql = "DROP SEQUENCE IF EXISTS test.not_exists RESTRICT";
        executeDDL(sql);
        assertEquals(Collections.singletonList(MessageFormat.format(ErrorCode.NO_SUCH_SEQUENCE.getMessage(), "test", "not_exists")), getWarnings());
    }
    
}
