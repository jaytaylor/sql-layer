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
