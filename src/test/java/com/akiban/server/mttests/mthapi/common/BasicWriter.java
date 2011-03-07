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

package com.akiban.server.mttests.mthapi.common;

import com.akiban.ais.model.TableName;
import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.WriteThreadStats;
import com.akiban.server.mttests.mthapi.base.sais.SaisFK;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.service.session.Session;
import com.akiban.util.ArgumentValidation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.util.ThreadlessRandom.rand;
import static org.junit.Assert.assertEquals;

public class BasicWriter implements WriteThread {

    public interface RowGenerator {
        public Object[] initialRow(SaisTable table, int pseudoRandom);
        public void updateRow(Object[] lastrow, int pseudoRandom);
    }

    private static class TablesInfo {
        private Object[] fields;
        // TODO make these final
        public int tableId;
        public SaisTable saisTable;
    }

    private static class TablesHolder {
        public TablesInfo randomTable(int seed) {
            throw new UnsupportedOperationException();
        }
    }

    private final long msOfSetup;
    private final RowGenerator rowGenerator;

    private int writes = 0;
    private Integer customer;
    private Integer order;
    private Integer item;
    private final TablesHolder tablesHolder = new TablesHolder();

    public BasicWriter(RowGenerator rowGenerator) {
        this(rowGenerator, -1, false);
    }

    public BasicWriter(RowGenerator rowGenerator, long msOfSetup) {
        this(rowGenerator, msOfSetup, true);
    }

    private BasicWriter(RowGenerator rowGenerator, long msOfSetup, boolean checkMsSetup) {
        ArgumentValidation.notNull("rowGenerator", rowGenerator);
        if (checkMsSetup) {
            ArgumentValidation.isGTE("msOfSetup", msOfSetup, 1);
        }
        this.msOfSetup = msOfSetup;
        this.rowGenerator = rowGenerator;
    }

    @Override
    public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
            throws InvalidOperationException
    {
        final int[] tables = {customers(), orders(), items()};
        int seed = this.hashCode();

        while (keepGoing.get()) {
            // TODO
//            seed = writeRandomly(session, seed, tables, tableIDs, dml);
        }
    }

//    private int writeRandomly(Session session, int seed, int[] tables, int[] tableFirstCols, DMLFunctions dml)
//            throws InvalidOperationException
//    {
//        seed = rand(seed);
//        final int tableIndex = Math.abs(seed % 3);
//        final int tableId = tables[ tableIndex ];
//        tableFirstCols[tableIndex] += Math.abs(seed % maxPKIncrement) + 1;
//        seed = rand(seed);
//        final int secondInt = seed % maxFKValue;
//        dml.writeRow(session, ApiTestBase.createNewRow(tableId, tableFirstCols[tableIndex], secondInt));
//        ++writes;
//        return seed;
//    }

    @Override
    public WriteThreadStats getStats() {
        return new WriteThreadStats(writes, 0, 0);
    }

    @Override
    public final void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
            throws InvalidOperationException
    {
        setupDDLS(ddl, session);

        customer = ddl.getTableId(session, new TableName("s1", "c") );
        order = ddl.getTableId(session, new TableName("s1", "o") );
        item = ddl.getTableId(session, new TableName("s1", "i") );

        setupRows(session, dml);
    }

    protected void setupDDLS(DDLFunctions ddl, Session session) {
        // do nothing
    }

    protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
        long start = System.currentTimeMillis();
        int seed = (int)start;
        if (msOfSetup > 0) {
            do {
                seed = writeRandomly(session, rand(seed), dml);
            } while (System.currentTimeMillis() - start <= msOfSetup);
        }
    }

    private int writeRandomly(Session session, int pseudoRandom, DMLFunctions dml) throws InvalidOperationException {
        TablesInfo info = tablesHolder.randomTable(pseudoRandom);
        pseudoRandom = rand(pseudoRandom);
        if (info.fields == null) {
            Object[] fields = rowGenerator.initialRow(info.saisTable, pseudoRandom);
            assertEquals("number of fields for " + info.saisTable, info.saisTable.getFields().size(), fields.length);
            info.fields = fields;
        } else {
            rowGenerator.updateRow(info.fields, pseudoRandom);
        }
        NewRow row = ApiTestBase.createNewRow(info.tableId, info.fields);
        dml.writeRow(session, row);
        return pseudoRandom;
    }

    protected final void createTable(SaisTable table, DDLFunctions ddl, Session session, StringBuilder scratch) {
        scratch.setLength(0);
        scratch.append("CREATE TABLE ");
//        String ddlText = buildDDL(table, tables, scratch);

    }

    static String buildDDL(SaisTable table, Set<SaisTable> tables, StringBuilder builder) {
        // fields
        builder.append(table.getName()).append('(');
        Iterator<String> fields = table.getFields().iterator();
        while (fields.hasNext()) {
            String field = fields.next();
            builder.append(field).append(" int");
            if (fields.hasNext()) {
                builder.append(',');
            }
        }

        // PK
        if (table.getPK() != null) {
            builder.append(", PRIMARY KEY ");
            cols(table.getPK().iterator(), builder);
        }

        // AkibanFK: CONSTRAINT __akiban_fk_FOO FOREIGN KEY __akiban_fk_FOO(pid1,pid2) REFERENCES parent(id1,id2)
        SaisFK parentFK = table.getParentFK();
        if (parentFK != null) {
            builder.append(", CONSTRAINT ");
            akibanFK(table, builder).append(" FOREIGN KEY ");
            akibanFK(table, builder);
            cols(parentFK.getChildCols(), builder).append(" REFERENCES ").append(parentFK.getParent().getName());
            cols(parentFK.getParentCols(), builder);
        }

        return builder.append(')').toString();
    }

    private static StringBuilder cols(Iterator<String> columns, StringBuilder builder) {
        builder.append('(');
        while (columns.hasNext()) {
            builder.append(columns.next());
            if (columns.hasNext()) {
                builder.append(',');
            }
        }
        builder.append(')');
        return builder;
    }

    private static StringBuilder akibanFK(SaisTable child, StringBuilder builder) {
        return builder.append("`__akiban_fk_").append(child.getName()).append('`');
    }

    protected final int customers() {
        return customer;
    }

    protected final int orders() {
        return order;
    }

    protected final int items() {
        return item;
    }
}
