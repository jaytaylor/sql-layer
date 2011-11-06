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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.ais.model.TableName;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.error.InvalidOperationException;
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
        ArgumentValidation.notNull("rowGenerator", rowGenerator);
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
        ddl.createTable(session, schema(), ddlText);
        int id = ddl.getTableId(session, new TableName(schema(), table.getName()));
        TableInfo info = new TableInfo(table, statesCount, id);
        tablesHolder.addTable(info);
    }

}
