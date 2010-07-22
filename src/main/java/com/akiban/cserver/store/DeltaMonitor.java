/**
 * 
 */
package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.persistit.KeyState;

/**
 * @author percent
 * 
 */
public class DeltaMonitor implements CommittedUpdateListener {

    public static class DeltaCursor {
        public DeltaCursor(PriorityQueue<Delta> queue) {
            assert queue != null;
            this.queue = queue;
        }

        public int size() {
            return queue.size();
        }

        public boolean check(KeyState nextKey) {
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

        private PriorityQueue<Delta> queue;
    }

    public DeltaMonitor(VStore vstore) {
        inserts = new TreeMap<Integer, ArrayList<Delta>>();
        rwLock = new ReentrantReadWriteLock();
        this.vstore = vstore;
        count = 0;
    }

    public HashSet<RowDef> getTables() {
        HashSet<RowDef> tables = new HashSet<RowDef>();
        Iterator<ArrayList<Delta>> i = inserts.values().iterator();
        while (i.hasNext()) {
            Iterator<Delta> j = i.next().iterator();
            while (j.hasNext()) {
                Delta d = j.next();
                tables.add(d.getRowDef());
            }
        }
        return tables;
    }

    public DeltaCursor createInsertCursor() {
        PriorityQueue<Delta> queue = new PriorityQueue<Delta>();
        Iterator<ArrayList<Delta>> i = inserts.values().iterator();
        while (i.hasNext()) {
            Iterator<Delta> j = i.next().iterator();
            while (j.hasNext()) {
                queue.add(j.next());
            }
        }
        return new DeltaCursor(queue);
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

        // XXX - hack for demonstration purposes
        if (!rowDef.getSchemaName().equals("toy_test")) {
            return;
        }

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
        count++;
        //System.out.println("Count = " + count);
        // XXX - Writing the V's should be a background task, and
        // not while holding a write lock that blocks the entire system. However
        // there are other questions that can be answered first (such as how do
        // we decide to write the deltas). This implementation servers as a
        // simple, concrete basis to work from while some of the background
        // elements gain more focus.
        if (count == vstore.getDeltaThreshold()) {
            VDeltaWriter dwriter = new VDeltaWriter(vstore.getDataPath(),
                    vstore.getVMeta(), this.createInsertCursor(), this
                            .getTables());
            try {
                dwriter.write();
            } catch (Exception e) {
                e.printStackTrace();
                throw new Error(
                        "----------- Failed to write deltas -----------");
            }
            vstore.setVMeta(dwriter.getMeta());
            inserts = new TreeMap<Integer, ArrayList<Delta>>();
            count = 0;
        }
        assert count < vstore.getDeltaThreshold();
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
    VStore vstore;
    int count;
}
