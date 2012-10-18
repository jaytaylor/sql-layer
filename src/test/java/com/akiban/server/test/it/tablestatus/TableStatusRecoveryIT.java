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

package com.akiban.server.test.it.tablestatus;

import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.concurrent.Callable;

import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.server.test.it.ITBase;
import org.junit.Test;

import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatus;
import com.akiban.server.service.config.Property;
import com.persistit.Persistit;

public class TableStatusRecoveryIT extends ITBase {
    
    @Test
    public void simpleInsertRowCountTest() throws Exception {
        int tableId = createTable("test", "A", "I INT NOT NULL, V VARCHAR(255), PRIMARY KEY(I)");
        for (int i = 0; i < 10000; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }
        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts1.getRowCount());
        
        crashAndRestartSaveDatapath();
        
        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts2.getRowCount());

    }

    @Test
    public void pkLessInsertRowCountTest() throws Exception {
        final int tableId = createTable("test", "A", "I INT, V VARCHAR(255)");
        for (int i = 0; i < 10000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(10000, ts1.getRowCount());

        for (int i = 10000; i < 20000; i++) {
            // -1: Dummy value for akiban-supplied PK
            writeRows(createNewRow(tableId, i, "This is record # " + 1, -1));
        }
        
        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(20000, ts2.getRowCount());

        crashAndRestartSaveDatapath();

        final TableStatistics ts3 = dml().getTableStatistics(session(), tableId, false);
        assertEquals(20000, ts3.getRowCount());

        // Transaction so we can directly read the table status
        transactionally(new Callable<Void>() {
            public Void call() throws Exception {
                final TableStatus status = store().getRowDef(session(), tableId).getTableStatus();
                assertEquals(20000, status.getRowCount());
                writeRows(createNewRow(tableId, 20001, "This is record # ", -1));
                assertEquals(20001, status.getUniqueID());
                return null;
            }
        });
    }

    @Test
    public void autoIncrementInsertTest() throws Exception {
        final int INSERT_COUNT = 10000;

        NewAISBuilder builder = AISBBasedBuilder.create("test");
        builder.userTable("A").autoIncLong("I", 1).colString("V", 255).pk("I");
        ddl().createTable(session(), builder.ais().getUserTable("test", "A"));
        updateAISGeneration();

        int tableId = tableId("test", "A");
        for (int i = 1; i <= INSERT_COUNT; i++) {
            writeRows(createNewRow(tableId, i, "This is record # " + 1));
        }

        final TableStatistics ts1 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count before restart", INSERT_COUNT, ts1.getRowCount());
        assertEquals("auto inc before restart", INSERT_COUNT, ts1.getAutoIncrementValue());

        crashAndRestartSaveDatapath();

        final TableStatistics ts2 = dml().getTableStatistics(session(), tableId, false);
        assertEquals("row count after restart", INSERT_COUNT, ts2.getRowCount());
        assertEquals("auto inc after restart", INSERT_COUNT, ts2.getAutoIncrementValue());
    }

    @Test
    public void ordinalCreationTest() throws Exception {
        final int aId = createTable("test", "A", "ID INT NOT NULL, PRIMARY KEY(ID)");
        final int aOrdinal = getOrdinal(aId);

        final int bId = createTable("test", "B", "ID INT NOT NULL, AID INT, PRIMARY KEY(ID)", akibanFK("AID", "A", "ID"));
        final int bOrdinal = getOrdinal(bId);
        
        assertEquals("ordinals unique before restart", true, aOrdinal != bOrdinal);
        
        crashAndRestartSaveDatapath();

        assertEquals("parent ordinal same after restart", aOrdinal, getOrdinal(aId));
        assertEquals("child ordinal same after restart", bOrdinal, getOrdinal(bId));
        
        final int cId = createTable("test", "C", "ID INT NOT NULL, BID INT, PRIMARY KEY(ID)", akibanFK("BID", "B", "ID"));
        final int cOrdinal = getOrdinal(cId);
        
        assertEquals("new grandchild after restart has unique ordinal", true, cOrdinal != aOrdinal && cOrdinal != bOrdinal);
    }

    private void crashAndRestartSaveDatapath() throws Exception {
        final Persistit db = serviceManager().getTreeService().getDb();

        final String datapath = db.getProperty("datapath");
        db.force();
        crashTestServices();

        final Property property = new Property("akserver.datapath", datapath);
        restartTestServices(Collections.singleton(property));
    }
    
    private int getOrdinal(final int tableId) throws Exception {
        return transactionally(new Callable<Integer>() {
            public Integer call() throws Exception {
                return store().getRowDef(session(), tableId).getTableStatus().getOrdinal();
            }
        });
    }
}
