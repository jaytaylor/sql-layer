/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server;

import com.foundationdb.server.error.PersistitAdapterException;
import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Tree;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;

public class AccumulatorAdapter {

    public static long getSnapshot(AccumInfo accumInfo, Tree tree) throws PersistitInterruptedException {
        Accumulator accumulator = getAccumulator(accumInfo, tree);
        return accumulator.getSnapshotValue();
    }

    public static void sumAdd(AccumInfo accumInfo, Exchange exchange, long value) {
        Accumulator.SumAccumulator sum = (Accumulator.SumAccumulator)getAccumulator(accumInfo, exchange.getTree());
        sum.add(value);
    }
    
    public static long getLiveValue(AccumInfo accumInfo, Tree tree) {
        Accumulator accumulator = getAccumulator(accumInfo, tree);
        return accumulator.getLiveValue();
    }

    public long getSnapshot() throws PersistitInterruptedException {
        return accumulator.getSnapshotValue();
    }

    public void sumAdd(long value) {
        Accumulator.SumAccumulator sum = (Accumulator.SumAccumulator)accumulator;
        sum.add(value);
    }

    public long seqAllocate() {
        Accumulator.SeqAccumulator seq = (Accumulator.SeqAccumulator)accumulator;
        return seq.allocate();
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
            this.sumAdd(diff);
        }
    }

    public AccumulatorAdapter(AccumInfo accumInfo, Tree tree) {
        this.accumulator = getAccumulator(accumInfo, tree);
    }

    private static Accumulator getAccumulator(AccumInfo accumInfo, Tree tree) {
        try {
            switch(accumInfo.type) {
                case SUM: return tree.getSumAccumulator(accumInfo.getIndex());
                case MAX: return tree.getMaxAccumulator(accumInfo.getIndex());
                case MIN: return tree.getMinAccumulator(accumInfo.getIndex());
                case SEQ: return tree.getSeqAccumulator(accumInfo.getIndex());
                default:
                    throw new IllegalStateException("Unknown accumulator type: " + accumInfo.type);
            }
        } catch (PersistitException e) {
            throw new PersistitAdapterException(e);
        }
    }

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
        // Previously used for ordinals. Replaced by storage on Table in AIS.
        //ORDINAL(0, Accumulator.Type.SUM),
        /** Size of a table or group index. Attached to PK tree and GI tree, respectively */
        ROW_COUNT(1, Accumulator.Type.SUM),
        /** Source of values for hidden primary keys. Attached to the PK tree. */
        UNIQUE_ID(2, Accumulator.Type.SEQ),
        /** Saves values from the MySQL adapter AUTO INCREMENT columns. Attached to the PK tree. */
        AUTO_INC(3, Accumulator.Type.SUM),
        /** Source of values for SQL sequences. Attached to the sequence tree. */
        SEQUENCE(4, Accumulator.Type.SEQ)
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
