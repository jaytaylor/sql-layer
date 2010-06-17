/**
 * 
 */
package com.akiban.cserver.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.RowDefCache;
import com.persistit.KeyState;

/**
 * @author percent
 * 
 */
public class DeltaMonitor implements CommittedUpdateListener {

    public static class DeltaCursor {
        public DeltaCursor(PriorityQueue<Delta> queue) {
            this.queue = queue;
        }

        public boolean check(KeyState nextKey) {
            assert queue != null;
            boolean ret = false;
            if (queue.size() == 0) {
                return ret;
            }

            Delta d = queue.peek();
            int preceeds = d.getKey().compareTo(nextKey);
            assert preceeds != 0;
            if (preceeds < 0) {
                ret = true;
            }
            return ret;
        }
        
        public Delta get() {
            return queue.peek();
        }
        
        public Delta remove() {
            assert queue != null && queue.size() > 0;
            return queue.poll();
        }

        PriorityQueue<Delta> queue;
    }

    public DeltaMonitor() {
        inserts = new TreeMap<Integer, ArrayList<Delta>>();
        rwLock = new ReentrantReadWriteLock();
    }

    public DeltaCursor createInsertCursor(ArrayList<Integer> tableIds) {

        PriorityQueue<Delta> queue = new PriorityQueue<Delta>();
        Iterator<Integer> i = tableIds.iterator();

        while (i.hasNext()) {
            int tableId = i.next().intValue();
            if (inserts.get(tableId) != null) {
                Iterator<Delta> j = inserts.get(tableId).iterator();
                while (j.hasNext()) {
                    queue.add(j.next());
                }
            }
        }
        return new DeltaCursor(queue);
    }

    public void readLock() {
        rwLock.readLock().lock();
    }

    public void releaseReadLock() {
        assert rwLock.getReadHoldCount() > 0;
        rwLock.readLock().unlock();
    }

    @Override
    public void inserted(KeyState keyState, RowDef rowDef, RowData rowData) {
        Delta newDelta = new Delta(Delta.Type.Insert, keyState, rowDef, rowData);
        // XXX - This is a big-ass hammer lock. This should probably be more
        // fine grained.
        rwLock.writeLock().lock();
        if (inserts.get(rowDef.getRowDefId()) == null) {
            inserts.put(rowDef.getRowDefId(), new ArrayList<Delta>());
        }
        assert inserts.get(rowDef.getRowDefId()) != null;
        boolean success = inserts.get(rowDef.getRowDefId()).add(newDelta);
        assert success;
        rwLock.writeLock().unlock();
    }

    @Override
    public void updated(KeyState keyState, RowDef rowDef, RowData oldRowData,
            RowData newRowData) {
    }

    @Override
    public void deleted(KeyState keyState, RowDef rowDef, RowData rowData) {
    }

    TreeMap<Integer, ArrayList<Delta>> inserts;
    // TreeMap<Integer, PriorityQueue<Delta>> updates;
    // TreeMap<Integer, PriorityQueue<Delta>> deletes;
    ReentrantReadWriteLock rwLock;
}
