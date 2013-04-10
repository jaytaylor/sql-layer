/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.sql.aisddl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.akiban.ais.model.Sequence;
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

    @Test
    public void durableAfterRollbackAndRestart() throws Exception {
        String sql = "CREATE SEQUENCE test.s1 START WITH 1 INCREMENT BY 1";
        executeDDL(sql);
        Sequence s1 = ais().getSequence(new TableName("test", "s1"));
        assertNotNull("s1", s1);

        txnService().beginTransaction(session());
        assertEquals("start val a", 0, s1.currentValue(treeService()));
        assertEquals("next val a", 1, s1.nextValue(treeService()));
        txnService().commitTransaction(session());

        txnService().beginTransaction(session());
        assertEquals("next val b", 2, s1.nextValue(treeService()));
        assertEquals("cur val b", 2, s1.currentValue(treeService()));
        txnService().rollbackTransactionIfOpen(session());

        txnService().beginTransaction(session());
        assertEquals("cur val c", 1, s1.currentValue(treeService()));
        // Expected gap, see nextValue() impl
        assertEquals("next val c", 3, s1.nextValue(treeService()));
        txnService().commitTransaction(session());

        safeRestartTestServices();

        txnService().beginTransaction(session());
        assertEquals("cur val after restart", 3, s1.currentValue(treeService()));
        assertEquals("next val after restart", 4, s1.nextValue(treeService()));
        txnService().commitTransaction(session());
    }
}
