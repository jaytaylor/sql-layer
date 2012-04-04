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

package com.akiban.server;

import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

public class AccumulatorAdapter {

    public static long getSnapshot(AccumInfo accumInfo, TreeService treeService, Tree tree)
    throws PersistitInterruptedException
    {
        Accumulator accumulator = getAccumulator(accumInfo, tree);
        Transaction txn = getCurrentTrx(treeService);
        return accumulator.getSnapshotValue(txn);
    }

    public static long updateAndGet(AccumInfo accumInfo, Exchange exchange, long value) {
        Accumulator accumulator = getAccumulator(accumInfo, exchange.getTree());
        return accumulator.update(value, exchange.getTransaction());
    }
    
    public static long getLiveValue(AccumInfo accumInfo, TreeService treeService, Tree tree)
    {
        Accumulator accumulator = getAccumulator(accumInfo, tree);
        return accumulator.getLiveValue();
    }

    public long getSnapshot() throws PersistitInterruptedException {
        return accumulator.getSnapshotValue(getCurrentTrx());
    }

    public long updateAndGet(long value) {
        return accumulator.update(value, getCurrentTrx());
    }

    public long getLiveValue() {
        return accumulator.getLiveValue();
    }

    public void set(long value) throws PersistitInterruptedException {
        set(value, true);
    }

    void set(long value, boolean evenIfLess) throws PersistitInterruptedException {
        long current = getSnapshot();
        if(evenIfLess || value > current) {
            long diff = value - current;
            this.updateAndGet(diff);
        }
    }

    public AccumulatorAdapter(AccumInfo accumInfo, TreeService treeService, Tree tree) {
        this.treeService = treeService;
        this.accumulator = getAccumulator(accumInfo, tree);
    }

    private static Accumulator getAccumulator(AccumInfo accumInfo, Tree tree) {
        try {
            return tree.getAccumulator(accumInfo.getType(), accumInfo.getIndex());
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

    private Transaction getCurrentTrx() {
        return getCurrentTrx(treeService);
    }

    private static Transaction getCurrentTrx(TreeService treeService) {
        return treeService.getDb().getTransaction();
    }

    private final TreeService treeService;
    private final Accumulator accumulator;

    /**
     * Mapping of indexes and types for the Accumulators used by the table status.
     * <p>
     * Note: Remember that <i>any</i> modification to existing values is an
     * <b>incompatible</b> data format change. It is only safe to stop using
     * an index position or add new ones at the end of the range.
     * </p>
     */
    public static enum AccumInfo {
        ORDINAL(0, Accumulator.Type.SUM),
        ROW_COUNT(1, Accumulator.Type.SUM),
        UNIQUE_ID(2, Accumulator.Type.SEQ),
        AUTO_INC(3, Accumulator.Type.SUM),
        ;
    
        AccumInfo(int index, Accumulator.Type type) {
            this.index = index;
            this.type = type;
        }
    
        private int getIndex() {
            return index;
        }
    
        private Accumulator.Type getType() {
            return type;
        }
    
        private final int index;
        private final Accumulator.Type type;
    }
}
