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

package com.akiban.server.test.mt.mthapi.coi;

import com.akiban.server.api.HapiRequestException;
import com.akiban.server.test.mt.mthapi.base.HapiMTBase;
import com.akiban.server.test.mt.mthapi.base.HapiSuccess;
import com.akiban.server.test.mt.mthapi.base.sais.SaisBuilder;
import com.akiban.server.test.mt.mthapi.base.sais.SaisTable;
import com.akiban.server.test.mt.mthapi.common.BasicHapiSuccess;
import com.akiban.server.test.mt.mthapi.common.BasicWriter;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;
import java.util.Set;

import static com.akiban.util.ThreadlessRandom.rand;

public final class CoiMT extends HapiMTBase {
    final static Set<SaisTable> COI_ROOTS = coi();

    @Test
    public void concurrentWritesWithOrphans() throws HapiRequestException, JSONException, IOException {
        BasicWriter writeThread = new BasicWriter(COI_ROOTS, allowOrphans(COI_ROOTS), store());
        HapiSuccess readThread = new BasicHapiSuccess(writeThread.schema(), COI_ROOTS, true);

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
            int id = initialPK(state);
            if (table.equals(customer)) {
                return new Object[]{id};
            }
            if (table.equals(order) || table.equals(item) ) {
                return new Object[]{id, parentId(id, pseudoRandom)};
            }
            throw new AssertionError(table);
        }

        protected int initialPK(byte state) {
            //return idForState(Math.abs(pseudoRandom % 50) + 1, state);
            return state;
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
