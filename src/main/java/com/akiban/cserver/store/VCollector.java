/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 */

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import com.akiban.vstore.*;
import com.akiban.cserver.*;
import com.akiban.cserver.store.DeltaMonitor.DeltaCursor;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

import java.util.*;

public class VCollector implements RowCollector {

    public VCollector(VMeta meta, DeltaMonitor dm,
            final RowDefCache rowDefCache, final int rowDefId,
            final byte[] columnBitMap) throws IOException {
        assert columnBitMap != null;
        deltaMonitor = dm;
        hasMore = true;
        rowIndex = 0;
        table = rowDefCache.getRowDef(rowDefId);
        cache = rowDefCache;
        assert table != null;
        
        userTables = new TreeMap<Integer, RowDef>();
        projection = new BitSet(table.getFieldCount());
        nullMap = new BitSet(table.getFieldCount());
        configureBitMap(columnBitMap);
        assert !projection.isEmpty();
        
        if (meta == null) {
            totalRows = 0;
            if(!table.isGroupTable()) {
                userTables.put(table.getRowDefId(), table);
            } else {
                findUserTableIds();
            }
        } else {
            fields = 0;
            keyQueue = new PriorityQueue<KeyState>();
            keyMap = new TreeMap<KeyState, TableDescriptor>();
            if (!table.isGroupTable()) {
                configureUserTableCollector(meta, table, 0, 
                        table.getFieldCount());
            } else {
                configureGroupTableCollector(meta);
            }
        }
        assert userTables.size() > 0;
        if(deltaMonitor != null) {
            createInsertCursor();
        }
    }

    public void createInsertCursor() {
        // XXX - this is horrendous. we have a potential dead lock here.
        // if the VCollector.collectNext row is not called until completion
        // the delta will not be unlocked.
        deltaMonitor.readLock();

        Iterator<Integer> i = userTables.keySet().iterator();
        ArrayList<Integer> tableIds = new ArrayList<Integer>();
        while (i.hasNext()) {
            int tableId = i.next().intValue();
            tableIds.add(tableId);
        }
        insertCursor = deltaMonitor.createInsertCursor(tableIds);
    }

    public void configureBitMap(final byte[] columnBitMap) {
        for (int i = 0; i < table.getFieldCount(); i++) {
            if ((columnBitMap[i / 8] & (1 << (i % 8))) != 0) {
                projection.set(i, true);
                fields++;
            } else {
                // System.out.println("VCollector: null column index = "+i);
                nullMap.set(i, true);
            }
        }
        // System.out.println("VCollector: Null map = "+nullMap);
    }

    public void findUserTableIds()
            throws FileNotFoundException, IOException {
        for (int i = 0; i < table.getUserTableRowDefs().length; i++) {
            final RowDef utable = table.getUserTableRowDefs()[i];
            int offset = utable.getColumnOffset();
            int distance = offset + utable.getFieldCount();
            for (int j = offset, k = 0; j < distance; j++, k++) {
                if (projection.get(j)) {
                    userTables.put(utable.getRowDefId(), utable);
                }
            }
            assert distance <= table.getFieldCount();
        }
    }
    
    public void configureGroupTableCollector(VMeta meta)
            throws FileNotFoundException, IOException {
        // ArrayList<TableDescriptor> tables = new ArrayList<TableDescriptor>();

        for (int i = 0; i < table.getUserTableRowDefs().length; i++) {
            final RowDef utable = table.getUserTableRowDefs()[i];
            int offset = utable.getColumnOffset();
            int distance = offset + utable.getFieldCount();
            // System.out.println("VCollector: " + utable.getTableName()
            // + " column offset = " + utable.getColumnOffset()
            // + " tableID = " + utable.getRowDefId()
            // + " group tableID = " + table.getRowDefId());
            configureUserTableCollector(meta, utable, offset, distance);
            assert distance <= table.getFieldCount();
        }
    }

    public void configureUserTableCollector(VMeta meta, RowDef utable,
            int offset, int distance) throws FileNotFoundException, IOException {
        TableDescriptor candidate = null;
        for (int j = offset, k = 0; j < distance; j++, k++) {
            if (projection.get(j)) {
                if (candidate == null) {
                    System.out.println("looking up HKey for descriptor = "+utable.getTableName());
                    IColumnDescriptor kdes = meta.getHKey(utable.getRowDefId());
                    assert kdes != null;
                    candidate = new TableDescriptor(kdes, utable
                            .getParentRowDefId(), utable.getRowDefId());
                }
                 //System.out.println("VCollector: " + utable.getTableName()
                 //+ ", fieldname: " + utable.getFieldDef(k).getName()
                 //+ ", k = " + k + ", j = "+ j +", rowDefId = "
                 //+ utable.getRowDefId() + ", fixedSize = "
                 //+ utable.getFieldDef(k).isFixedSize());
                
                IColumnDescriptor cdes = meta.lookup(utable.getRowDefId(), k);
                assert cdes != null;
                assert candidate != null;
                candidate.add(cdes);
            }
        }

        // XXX - h4x0r
        if (candidate != null) {
            assert candidate.getFieldArrayList().size() > 0;
            totalRows += candidate.getRows();
            userTables.put(utable.getRowDefId(), utable);
            // tables.add(candidate);
            List<byte[]> keyBytes = candidate.scanKeys();
            Iterator<byte[]> k = keyBytes.iterator();
            while (k.hasNext()) {
                byte[] key = k.next();
                Key pk = new Key((Persistit) null);
                KeyState ks = new KeyState(key);
                pk.clear();
                ks.copyTo(pk);
                // System.out.println("VCollector: Table = "
                // + utable.getTableName() + " key = " + pk);
                keyMap.put(ks, candidate);
                keyQueue.add(ks);
            }
        }
    }

    public BitSet getProjection() {
        return projection;
    }

    public Collection<RowDef> getUserTables() {
        return userTables.values();
    }

    /*
     * public Tree<TableDescriptor> getHierarchy() { return hierarchy; }
     */

    @Override
    public void close() {
        assert false;
    }

    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception {
        // assert hasMore == true;
        if (!hasMore) {
            return false;
        }

        int chunkDepth = payload.position();
        // System.out.println("chunk depth = " + chunkDepth);
        RowData row = new RowData();
        boolean scannedARow = false;

        while (rowIndex < totalRows) {
            KeyState nextKey = keyQueue.peek();
            assert nextKey != null;

            if (deltaMonitor != null && insertCursor.check(nextKey)) {
                assert false;
                Delta d = insertCursor.get();
                assert d != null;
                if (d.getRowData().getRowSize() > (payload.limit() - payload
                        .position())) {
                    break;
                }
                insertCursor.remove();

                int offset = 0;
                if (table.isGroupTable()) {
                    offset = d.getRowDef().getColumnOffset();
                }

                row.reset(payload.array(), chunkDepth, d.getRowData()
                        .getRowSize());
                row.copy(d.getRowDef(), d.getRowData(), nullMap, offset);
                scannedARow = true;
                chunkDepth += d.getRowData().getRowSize();
                payload.position(d.getRowData().getRowSize()
                        + payload.position());

            } else {
                List<FieldArray> fields = keyMap.get(nextKey)
                        .getFieldArrayList();
                RowDef rowDef = userTables
                        .get(keyMap.get(nextKey).getTableId());

                int nextRowSize = 0;
                for (int i = 0; i < fields.size(); i++) {
                    nextRowSize += fields.get(i).getNextFieldSize();
                }
                assert nextRowSize > 0;

                nextRowSize += RowData.MINIMUM_RECORD_LENGTH
                        + (rowDef.getFieldCount() % 8 == 0 ? rowDef
                                .getFieldCount() / 8
                                : rowDef.getFieldCount() / 8 + 1);

                if (nextRowSize > (payload.limit() - payload.position())) {
                    break;
                }

                // System.out.println("VCollector: table = "
                // +
                // userTables.get(keyMap.get(nextKey).getTableId()).getTableName()
                // + ", key = " + pkey
                // + ", chunkDepth = " + chunkDepth + ", nextRowSize = "
                // + nextRowSize + " fields =" + fields.size());
                int offset = 0;
                if (table.isGroupTable()) {
                    offset = userTables.get(keyMap.get(nextKey).getTableId())
                            .getColumnOffset();
                }

                keyQueue.poll();
                row.reset(payload.array(), chunkDepth, nextRowSize);
                row.mergeFields(userTables
                        .get(keyMap.get(nextKey).getTableId()), fields,
                        nullMap, offset);
//              int k = chunkDepth;
//              while (k < chunkDepth + nextRowSize) {
//                  System.out.print(Integer.toHexString(payload.array()[k])+" ");
//                  k++;
//              }
//          System.out.println("VCollector decoded row = " + row.toString(cache));
                keyMap.get(nextKey).incrementCursor();
                scannedARow = true;
                chunkDepth += nextRowSize;
                payload.position(nextRowSize + payload.position());
                rowIndex++;
            }
        }

        if (rowIndex == totalRows && deltaMonitor != null) {
            hasMore = false;
            while (insertCursor.get() != null) {
                Delta d = insertCursor.get();
                assert d != null;
                if (d.getRowData().getRowSize() 
                        > (payload.limit() - payload.position())) {
                    break;
                }
                insertCursor.remove();

                int offset = 0;
                if (table.isGroupTable()) {
                    offset = d.getRowDef().getColumnOffset();
                }

                row.reset(payload.array(), chunkDepth, d.getRowData()
                        .getRowSize());
                row.copy(d.getRowDef(), d.getRowData(), nullMap, offset);
                scannedARow = true;
                chunkDepth += d.getRowData().getRowSize();
                payload.position(d.getRowData().getRowSize()
                        + payload.position());
                
                //int k = chunkDepth - d.getRowData().getRowSize();
                //System.out.print("VCollector.LOG rowRawBytes = ");
                //while (k < chunkDepth) {
                //    System.out.print(Integer.toHexString(payload.array()[k])+" ");
                //    k++;
                //}
                //System.out.println();
                //System.out.println("VCollector decoded row = " + row.toString(cache));


                
            }
            // XXX - this is horrendous. We have a potential dead lock here.
            // If the rowCollector is not called until it is done
            // the delta will not be unlocked.
            deltaMonitor.releaseReadLock();
        } else if(rowIndex == totalRows) {
            hasMore = false;
        }
        
        //assert hasMore || scannedARow;
        return scannedARow;
    }

    @Override
    public boolean hasMore() throws Exception {
        // assert false;
        return hasMore;
    }

    @Override
    public void refreshAncestorRows() {
        assert false;
    }

    private boolean hasMore;
    private int totalRows;
    private int rowIndex;
    private int fields;
    private RowDefCache cache;
    private BitSet projection;
    private BitSet nullMap;
    private RowDef table;
    private DeltaMonitor deltaMonitor;
    private DeltaCursor insertCursor;

    private TreeMap<KeyState, TableDescriptor> keyMap;
    private TreeMap<Integer, RowDef> userTables;
    private PriorityQueue<KeyState> keyQueue;
}
