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
import com.akiban.server.service.session.Session;

import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.util.ThreadlessRandom.rand;

public class BasicWriter implements WriteThread {
    private final int MAX_INC;
    private final int MAX_INT;
    private final long msOfSetup;
    private final int[] tableIDs = {1, 1, 1};

    private int writes = 0;
    private Integer customer;
    private Integer order;
    private Integer item;

    public BasicWriter(int MAX_INC, int MAX_INT, long msOfSetup) {
        this.MAX_INC = MAX_INC;
        this.MAX_INT = MAX_INT;
        this.msOfSetup = msOfSetup;
    }

    protected void setupRows(Session session, DMLFunctions dml) throws InvalidOperationException {
        final int[] tables = {customers(), orders(), items()};
        long start = System.currentTimeMillis();
        int seed = (int)start;
        while (System.currentTimeMillis() - start <= msOfSetup) {
            seed = writeRandomly(session, seed, tables, tableIDs, dml);
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
        tableFirstCols[tableIndex] += Math.abs(seed % MAX_INC) + 1;
        seed = rand(seed);
        final int secondInt = seed % MAX_INT;
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
        ddl.createTable(session, "s1", "create table c(id int key, age int)");
        ddl.createTable(session, "s1", "create table o(id int key, cid int, "
                +" CONSTRAINT __akiban_o FOREIGN KEY __akiban_o (cid) REFERENCES c (id)"
                +" )");
        ddl.createTable(session, "s1", "create table i(id int key, oid int, "
                +" CONSTRAINT __akiban_i FOREIGN KEY __akiban_o (oid) REFERENCES o (id)"
                +" )");

        customer = ddl.getTableId(session, new TableName("s1", "c") );
        order = ddl.getTableId(session, new TableName("s1", "o") );
        item = ddl.getTableId(session, new TableName("s1", "i") );

        setupRows(session, dml);
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
