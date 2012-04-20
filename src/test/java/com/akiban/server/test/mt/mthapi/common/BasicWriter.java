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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.rowdata.SchemaFactory;
import com.akiban.server.store.Store;
import com.akiban.server.test.ApiTestBase;
import com.akiban.server.test.mt.mthapi.base.WriteThread;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.server.service.session.Session;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.akiban.util.ThreadlessRandom.rand;
import static org.junit.Assert.assertEquals;

public class BasicWriter implements WriteThread {

    private static final int WRITES_INITIAL = -1;

    public interface RowGenerator {
        byte getStatesCount();
        Object[] initialRow(SaisTable table, byte state, int pseudoRandom);
        void updateRow(SaisTable table, Object[] lastRow, byte state, int pseudoRandom);
        byte nextState(byte currentState);
    }

    private static class TableInfo {
        private final Object[][] _fields;
        public final int tableId;
        public final SaisTable saisTable;
        private byte state = 0;

        public TableInfo(SaisTable saisTable, byte statesCount, int tableId) {
            this.saisTable = saisTable;
            this.tableId = tableId;
            _fields = new Object[statesCount][];
        }

        public Object[] fieldsForState() {
            return _fields[state];
        }

        public void setFieldsForState(Object[] fields) {
            assert _fields[state] == null : Arrays.toString(_fields[state]);
            _fields[state] = fields;
        }
    }

    private static class TablesHolder {
        private final List<TableInfo> tables = new ArrayList<TableInfo>();

        public TableInfo randomTable(int seed) {
            if (tables.isEmpty()) {
                throw new NoSuchElementException("you must create tables before you can write rows!");
            }
            int index = Math.abs(seed % tables.size());
            return tables.get(index);
        }

        public void addTable(TableInfo tableInfo) {
            tables.add(tableInfo);
        }
    }

    private final byte statesCount;
    private final long msOfSetup;
    private final RowGenerator rowGenerator;

    private int writes = 0;
    private final TablesHolder tablesHolder = new TablesHolder();
    private AtomicInteger writesAtomic = new AtomicInteger(WRITES_INITIAL);
    private final Set<SaisTable> initialRoots;
    private final Store store;

    public BasicWriter(Set<SaisTable> initialRoots, RowGenerator rowGenerator, Store store) {
        this(initialRoots, rowGenerator, -1, false, store);
    }

    public BasicWriter(Set<SaisTable> initialRoots, RowGenerator rowGenerator, long msOfSetup, Store store) {
        this(initialRoots, rowGenerator, msOfSetup, true, store);
    }

    private BasicWriter(Set<SaisTable> initialRoots, RowGenerator rowGenerator, long msOfSetup, boolean checkMsSetup, Store store) {
        ArgumentValidation.notNull("iterationHelper", rowGenerator);
        if (checkMsSetup) {
            ArgumentValidation.isGTE("msOfSetup", msOfSetup, 1);
        }
        this.msOfSetup = msOfSetup;
        this.rowGenerator = rowGenerator;
        this.initialRoots = new HashSet<SaisTable>(initialRoots);
        this.statesCount = rowGenerator.getStatesCount();
        this.store = store;
    }

    @Override
    public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
            throws InvalidOperationException
    {
        int seed = this.hashCode();

        while (keepGoing.get()) {
            seed = writeRandomly(session, seed, dml, store);
        }
        boolean writesWasUnset = writesAtomic.compareAndSet(WRITES_INITIAL, writes);
        assert writesWasUnset;
    }

    @Override
    public final void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
            throws InvalidOperationException
    {
        setupDDLS(ddl, session);
        setupRows(session, dml);
    }

    @Override
    public boolean continueThroughException(Throwable throwable) {
        return true;
    }

    protected void setupDDLS(DDLFunctions ddl, Session session) throws InvalidOperationException {
        Set<SaisTable> allTables = SaisTable.setIncludingChildren(initialRoots);
        StringBuilder sb = new StringBuilder();
        for (SaisTable table : allTables) {
            createTable(table, ddl, session, sb);
        }
    }

    protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
        long start = System.currentTimeMillis();
        int seed = (int)start;
        if (msOfSetup > 0) {
            do {
                seed = writeRandomly(session, rand(seed), dml, store);
            } while (System.currentTimeMillis() - start <= msOfSetup);
        }
    }

    private int writeRandomly(Session session, int pseudoRandom, DMLFunctions dml, Store store)
            throws InvalidOperationException
    {
        TableInfo info = tablesHolder.randomTable(pseudoRandom);
        pseudoRandom = rand(pseudoRandom);
        if (info.fieldsForState() == null) {
            Object[] fields = rowGenerator.initialRow(info.saisTable, info.state, pseudoRandom);
            assertEquals("number of fields for " + info.saisTable, info.saisTable.getFields().size(), fields.length);
            info.setFieldsForState(fields);
        } else {
            rowGenerator.updateRow(info.saisTable, info.fieldsForState(), info.state, pseudoRandom);
        }
        NewRow row = ApiTestBase.createNewRow(store, info.tableId, info.fieldsForState());
        dml.writeRow(session, row);
        info.state = rowGenerator.nextState(info.state);
        ++writes;
        return pseudoRandom;
    }

    public String schema() {
        return "ts1";
    }

    protected final void createTable(SaisTable table, DDLFunctions ddl, Session session, StringBuilder scratch)
            throws InvalidOperationException
    {
        String ddlText = DDLUtils.buildDDL(table, scratch);
        SchemaFactory schemaFactory = new SchemaFactory(schema());
        AkibanInformationSchema tempAIS = schemaFactory.ais(ddl.getAIS(session), ddlText);
        ddl.createTable(session, tempAIS.getUserTable(schema(), table.getName()));
        int id = ddl.getTableId(session, new TableName(schema(), table.getName()));
        TableInfo info = new TableInfo(table, statesCount, id);
        tablesHolder.addTable(info);
    }

}
