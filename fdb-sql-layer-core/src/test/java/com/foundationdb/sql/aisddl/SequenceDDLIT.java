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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.junit.Ignore;
import com.foundationdb.ais.model.Sequence;
import org.junit.Test;

import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.ErrorCode;
import com.foundationdb.server.error.NoSuchSequenceException;
import com.foundationdb.sql.parser.SQLParserException;
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
        
        dropSequence(new TableName("test", "new_sequence"));
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

        dropSequence (new TableName("test", "new_sequence"));
    }
    
    @Test (expected=NoSuchSequenceException.class)
    public void doubleDropSequence() throws Exception {
        int startNo = ais().getSequences().size();
        String sql = "CREATE SEQUENCE test.new_sequence";
        executeDDL(sql);
        assertNotNull (ais().getSequence(new TableName ("test", "new_sequence")));

        sql = "DROP SEQUENCE test.new_sequence restrict";
        executeDDL(sql);
        assertEquals (0, startNo - ais().getSequences().size());

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
        StoreAdapter adapter = newStoreAdapter();
        TableName seqName = new TableName("test", "s1");
        String sql = "CREATE SEQUENCE "+seqName+" START WITH 1 INCREMENT BY 1";
        executeDDL(sql);
        Sequence s1 = ais().getSequence(seqName);
        assertNotNull("s1", s1);

        txnService().beginTransaction(session());
        assertEquals("start val a", 0, adapter.sequenceCurrentValue(s1));
        assertEquals("next val a", 1, adapter.sequenceNextValue(s1));
        txnService().commitTransaction(session());

        txnService().beginTransaction(session());
        assertEquals("next val b", 2, adapter.sequenceNextValue(s1));
        assertEquals("cur val b", 2, adapter.sequenceCurrentValue(s1));
        txnService().rollbackTransactionIfOpen(session());

        txnService().beginTransaction(session());
        // Gap allowed, see nextValue() impl
        long nextVal1 = adapter.sequenceNextValue(s1);
        if(nextVal1 < 2) {
            fail("Expected next val >= 2: " + nextVal1);
        }
        txnService().commitTransaction(session());

        safeRestartTestServices();
        adapter = newStoreAdapter();

        s1 = ais().getSequence(seqName);
        txnService().beginTransaction(session());
        long nextVal2 = adapter.sequenceNextValue(s1);
        if(nextVal2 <= nextVal1) {
            fail("Expected next val > previous next val " + nextVal1 + ": " + nextVal2);
        }
        txnService().commitTransaction(session());
        dropSequence(seqName);
    }

    @Test
    public void freshValueAfterDropAndRecreate() throws Exception {
        StoreAdapter adapter = newStoreAdapter();
        final TableName seqName = new TableName("test", "s2");
        final String create = "CREATE SEQUENCE "+seqName+" START WITH 1 INCREMENT BY 1";
        final String drop = "DROP SEQUENCE "+seqName+" RESTRICT";
        for(int i = 1; i <= 2; ++i) {
            executeDDL(create);
            Sequence s1 = ais().getSequence(seqName);
            assertNotNull("s1, loop"+i, s1);

            txnService().beginTransaction(session());
            assertEquals("start val, loop"+i, 0, adapter.sequenceCurrentValue(s1));
            assertEquals("next val, loop"+i, 1, adapter.sequenceNextValue(s1));
            txnService().commitTransaction(session());

            executeDDL(drop);
        }
    }

    @Test
    public void unspecifiedDefaults() throws Exception {
        String sql = "CREATE SEQUENCE s";
        executeDDL(sql);
        Sequence s = ais().getSequence(new TableName ("test", "s"));
        assertNotNull(s);
        assertEquals("start", 1, s.getStartsWith());
        assertEquals("min", 1, s.getMinValue());
        assertEquals("max", Long.MAX_VALUE, s.getMaxValue());
        assertEquals("isCycle", false, s.isCycle());
    }

    @Test
    public void minButNoStart() throws Exception {
        String sql = "CREATE SEQUENCE s MINVALUE 10";
        executeDDL(sql);
        Sequence s = ais().getSequence(new TableName ("test", "s"));
        assertNotNull(s);
        assertEquals("start", 10, s.getStartsWith());
        assertEquals("min", 10, s.getMinValue());
    }

    @Test
    public void asInteger() throws Exception {
        String sql = "CREATE SEQUENCE s AS INTEGER";
        executeDDL(sql);
        Sequence s = ais().getSequence(new TableName ("test", "s"));
        assertNotNull(s);
        assertEquals("start", 1, s.getStartsWith());
        assertEquals("min", 1, s.getMinValue());
        assertEquals("max", Integer.MAX_VALUE, s.getMaxValue());
    }

    @Test
    public void wrapCacheSize() throws Exception {
        StoreAdapter adapter = newStoreAdapter();
        final TableName seqName = new TableName ("test", "s5");
        final String create = "CREATE SEQUENCE "+seqName+" START WITH 1 INCREMENT BY 1";
        executeDDL(create);
        Sequence s1 = ais().getSequence(seqName);
        for (int i = 1; i <= 103; ++i) {
            txnService().beginTransaction(session());
            assertEquals("loop cache size match", i, adapter.sequenceNextValue(s1));
            txnService().commitTransaction(session());
        }
        dropSequence(seqName);
    }
    
    private void dropSequence (TableName seqName) throws Exception {
        int startNo = ais().getSequences().size();
        executeDDL ("DROP SEQUENCE " + seqName + " RESTRICT");
        assertEquals (1, startNo - ais().getSequences().size());
    }
}
