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

package com.akiban.server.mttests.mthapi.coi;

import com.akiban.server.InvalidOperationException;
import com.akiban.server.api.DDLFunctions;
import com.akiban.server.api.DMLFunctions;
import com.akiban.server.api.HapiRequestException;
import com.akiban.server.mttests.mthapi.base.HapiMTBase;
import com.akiban.server.mttests.mthapi.base.HapiSuccess;
import com.akiban.server.mttests.mthapi.base.WriteThread;
import com.akiban.server.mttests.mthapi.base.sais.SaisBuilder;
import com.akiban.server.mttests.mthapi.base.sais.SaisTable;
import com.akiban.server.mttests.mthapi.common.BasicHapiSuccess;
import com.akiban.server.mttests.mthapi.common.BasicWriter;
import com.akiban.server.service.session.Session;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.akiban.util.ThreadlessRandom.rand;

public final class CoiMT extends HapiMTBase {
    final static int MAX_READ_ID = 10000;
    final static Set<SaisTable> COI_ROOTS = coi();

    @Test
    public void preWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(COI_ROOTS, allowOrphans(COI_ROOTS), 10000) {
            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                // do nothing
            }
        };
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void concurrentWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(COI_ROOTS, allowOrphans(COI_ROOTS));
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void concurrentWritesNoOrphans() throws HapiRequestException, JSONException, IOException {
        WriteThread writeThread = new BasicWriter(COI_ROOTS, allowOrphans(COI_ROOTS));
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);

        runThreads(writeThread, readThread);
    }

    @Test
    public void preWritesNoOrphans() throws HapiRequestException, JSONException, IOException {

        WriteThread writeThread = new BasicWriter(COI_ROOTS, allowOrphans(COI_ROOTS)) {
            @Override
            public void ongoingWrites(DDLFunctions ddl, DMLFunctions dml, Session session, AtomicBoolean keepGoing)
                    throws InvalidOperationException
            {
                // do nothing
            }
        };
        HapiSuccess readThread = new BasicHapiSuccess(MAX_READ_ID);
        runThreads(writeThread, readThread);
    }

    private static Set<SaisTable> coi() {
        SaisBuilder builder = new SaisBuilder();
        builder.table("customer", "cid").pk("cid");
        builder.table("orders", "oid", "c_id").pk("oid").joinTo("customer").col("cid", "c_id");
        builder.table("item", "iid", "o_id").pk("iid").joinTo("orders").col("oid", "o_id");
        return builder.getRootTables();
    }

    private static BasicWriter.RowGenerator allowOrphans(Set<SaisTable> coia) {
        return new COIWithOrphansWriter(coia.iterator().next());
    }

    private static class COIWithOrphansWriter implements BasicWriter.RowGenerator {
        private static final int INC_MAX = 10;
        private static final int STATES_COUNT = 9;

        private final SaisTable customer;
        private final SaisTable order;
        private final SaisTable item;

        private COIWithOrphansWriter(SaisTable customer) {
            this.customer = customer;
            order = customer.getChild("orders");
            item = order.getChild("item");
        }

        @Override
        public byte getStatesCount() {
            return STATES_COUNT;
        }

        private static int idForState(int id, byte state) {
            return (id * 10) + state;
        }

        @Override
        public Object[] initialRow(SaisTable table, byte state, int pseudoRandom) {
            //int id = idForState(Math.abs(pseudoRandom % 50) + 1, state);
            int id = state;
            if (table.equals(customer)) {
                return new Object[]{id};
            }
            if (table.equals(order) || table.equals(item) ) {
                return new Object[]{id, parentId(id, pseudoRandom)};
            }
            throw new AssertionError(table);
        }

        private int parentId(int pkId, int seed) {
            assert pkId >= 0 : pkId;
            int max = pkId * 2 + 5;
            return (Math.abs(rand(seed)) + 1) % max;
        }

        @Override
        public void updateRow(SaisTable table, Object[] lastRow, byte state, int pseudoRandom) {
            int increment = Math.abs(pseudoRandom % INC_MAX) + 1;

            final int lastId = (Integer) lastRow[0];
            assert lastId % 10 == state : String.format("%d != %d", lastId % 10, state);
            int id = lastId;
            id = idForState( (id / 10) + increment, state);
            assert id % 10 == state : String.format("%d != %d", id % 10, state);
            assert id > lastId : String.format("%d <= %d", id, lastId);


            lastRow[0] = id;
            if (table.equals(order) || table.equals(item)) {
                lastRow[1] = parentId(id, rand(pseudoRandom));
            }
        }

        @Override
        public byte nextState(byte currentState) {
            return (byte)( (currentState+1) % STATES_COUNT );
        }
    }
}
