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
import com.akiban.server.itests.ApiTestBase;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.WriteThreadStats;
import com.akiban.server.mttests.mthapi.base.sais.ParentFK;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.service.session.Session;
import com.akiban.util.ArgumentValidation;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.util.ThreadlessRandom.rand;

public class BasicWriter implements WriteThread {
    private final int maxPKIncrement;
    private final int maxFKValue;
    private final long msOfSetup;
    private final int[] tableIDs = {1, 1, 1};

    private int writes = 0;
    private Integer customer;
    private Integer order;
    private Integer item;

    public BasicWriter(int maxPKIncrement, int maxFKValue) {
        this.maxPKIncrement = maxPKIncrement;
        this.maxFKValue = maxFKValue;
        this.msOfSetup = -1;
    }

    public BasicWriter(int maxPKIncrement, int maxFKValue, long msOfSetup) {
        ArgumentValidation.isGTE("msOfSetup", msOfSetup, 1);
        this.maxPKIncrement = maxPKIncrement;
        this.maxFKValue = maxFKValue;
        this.msOfSetup = msOfSetup;
    }

    protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
        final int[] tables = {customers(), orders(), items()};
        long start = System.currentTimeMillis();
        int seed = (int)start;
        if (msOfSetup > 0) {
            do {
                seed = writeRandomly(session, seed, tables, tableIDs, dml);
            } while (System.currentTimeMillis() - start <= msOfSetup);
        }
    }

    @Override
    public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
            throws InvalidOperationException
    {
        final int[] tables = {customers(), orders(), items()};
        int seed = this.hashCode();

        while (keepGoing.get()) {
            seed = writeRandomly(session, seed, tables, tableIDs, dml);
        }
    }

    private int writeRandomly(Session session, int seed, int[] tables, int[] tableFirstCols, DMLFunctions dml)
            throws InvalidOperationException
    {
        seed = rand(seed);
        final int tableIndex = Math.abs(seed % 3);
        final int tableId = tables[ tableIndex ];
        tableFirstCols[tableIndex] += Math.abs(seed % maxPKIncrement) + 1;
        seed = rand(seed);
        final int secondInt = seed % maxFKValue;
        dml.writeRow(session, ApiTestBase.createNewRow(tableId, tableFirstCols[tableIndex], secondInt));
        ++writes;
        return seed;
    }

    @Override
    public WriteThreadStats getStats() {
        return new WriteThreadStats(writes, 0, 0);
    }

    @Override
    public final void setupWrites(DDLFunctions ddl, DMLFunctions dml, Session session)
            throws InvalidOperationException
    {
        SaisBuilder builder = new SaisBuilder();
        builder.table("c", "id", "age").pk("id");
        builder.table("o", "id", "cid").pk("id").joinTo("c").col("id", "cid");
        builder.table("i", "id", "oid").pk("id").joinTo("o").col("id", "oid");
        setupDDLS(builder.getAllTables(), ddl, session);

        customer = ddl.getTableId(session, new TableName("s1", "c") );
        order = ddl.getTableId(session, new TableName("s1", "o") );
        item = ddl.getTableId(session, new TableName("s1", "i") );

        setupRows(session, dml);
    }

    protected void setupDDLS(Set<SaisTable> tables, DDLFunctions ddl, Session session)
            throws InvalidOperationException
    {
        StringBuilder builder = new StringBuilder("CREATE TABLE ");
        final int baseLen = builder.length();

        for(SaisTable table : tables) {
            builder.setLength(baseLen);
            String ddlText = buildDDL(table, tables, builder);
            ddl.createTable(session, "s1", ddlText);
        }
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
        ParentFK parentFK = table.getParentFK(tables);
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
