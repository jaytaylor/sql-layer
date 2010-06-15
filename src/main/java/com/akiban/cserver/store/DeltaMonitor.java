/**
 * 
 */
package com.akiban.cserver.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
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
    
    public DeltaMonitor() {
       inserts = new TreeMap<Integer, PriorityQueue<Delta>>();
       rwLock = new ReentrantReadWriteLock();
    }
        
    public void configureInsertCursor(int tableId) {
        insertCursors.remove(tableId);
        insertCursors.put(tableId, new PriorityQueue<Delta>(inserts.get(tableId)));
    }
    
    public void setDeleteCursor(int tableId) {
        assert false;
    }
    
    public void startUpdateCursor(int tableId) {
        assert false;
    }
    
    public boolean mergeInsert(KeyState nextKey, int tableId, RowData rowData, BitSet nullMap, 
            int nullMapOffset) throws IOException {
        boolean ret = false;
        Delta d = insertCursors.get(tableId).peek();
        int preceeds = d.getKey().compareTo(nextKey);
        assert preceeds != 0; 
        if(preceeds < 0) {
            insertCursors.get(tableId).poll();
            rowData.copy(d.getRowDef(), d.getRowData(), nullMap, nullMapOffset);
            ret = true;
        } 
        return ret;
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
        // XXX - This is a big-ass hammer lock.  This should probably make this 
        //       more fine grained.
        rwLock.writeLock().lock();
        if(inserts.get(rowDef.getRowDefId()) == null) {
            inserts.put(rowDef.getRowDefId(), new PriorityQueue<Delta>());
        } 
        boolean success = inserts.get(rowDef.getRowDefId()).add(newDelta);
        assert success;
        rwLock.writeLock().unlock();
    }

    @Override
    public void updated(KeyState keyState, RowDef rowDef, RowData oldRowData, RowData newRowData) {
    }

    @Override
    public void deleted(KeyState keyState, RowDef rowDef, RowData rowData) {
    }
    
    // tableId == Integer
    TreeMap<Integer, PriorityQueue<Delta>> insertCursors;
    //TreeMap<Integer, PriorityQueue<Delta>> deleteCursors;
    //TreeMap<Integer, PriorityQueue<Delta>> updateCursors;
    TreeMap<Integer, PriorityQueue<Delta>> inserts;
    //TreeMap<Integer, PriorityQueue<Delta>> updates;
    //TreeMap<Integer, PriorityQueue<Delta>> deletes;
    ReentrantReadWriteLock rwLock;
}
