/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 *
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import com.akiban.vstore.*;
import com.akiban.cserver.*;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

import java.util.*;

public class VCollector implements RowCollector {

    public VCollector(VMeta meta, final RowDefCache rowDefCache,
            final int rowDefId, final byte[] columnBitMap) throws IOException {
        assert columnBitMap != null;
        assert meta != null;

        hasMore = true;
        fields = 0;
        userTables = null;

        table = rowDefCache.getRowDef(rowDefId);
        projection = new BitSet(table.getFieldCount());
        nullMap = new BitSet(table.getFieldCount());
        keyQueue = new PriorityQueue<KeyState>();
        keyMap = new TreeMap<KeyState, TableDescriptor>();

        assert table != null;
        projection.clear();
        nullMap.clear();

        for (int i = 0; i < table.getFieldCount(); i++) {
            if ((columnBitMap[i / 8] & (1 << (i % 8))) != 0) {
                projection.set(i, true);
                fields++;
            } else {
//                System.out.println("index = "+i);
                assert false;
                nullMap.set(i, true);
            }
        }

        if (!table.isGroupTable()) {
            table = rowDefCache.getRowDef(table.getGroupRowDefId());
        }

        ArrayList<TableDescriptor> tables = new ArrayList<TableDescriptor>();
        userTables = new TreeMap<Integer, RowDef>();
        for (int i = 0; i < table.getUserTableRowDefs().length; i++) {

            final RowDef utable = table.getUserTableRowDefs()[i];
            TableDescriptor candidate = null;

            int offset = utable.getColumnOffset();
            int distance = offset + utable.getFieldCount();
            assert distance <= table.getFieldCount();
            for (int j = offset, k = 0; j < distance; j++, k++) {
                if (projection.get(j)) {

                    if(candidate == null) {
                        IColumnDescriptor kdes = meta.getHKey(utable.getRowDefId());
                        assert kdes != null;
                        candidate = new TableDescriptor(kdes, utable.getParentRowDefId(),
                                utable.getRowDefId());
                    }
                    //System.out.println(utable.getTableName()+ ", fieldname: "
                    // +utable.getFieldDef(k).getName());
                    IColumnDescriptor cdes = meta.lookup(utable.getRowDefId(), k);
                    assert cdes != null;
                    assert candidate != null;
                    candidate.add(cdes);
                }
            }
            
            // XXX - h4x0r
            if(candidate != null) {
                assert candidate.getFieldArrayList().size() > 0;
                totalRows += candidate.getRows();
                userTables.put(utable.getRowDefId(), utable);
                tables.add(candidate);
                List<byte[]> keyBytes = candidate.scanKeys();
                Iterator<byte[]> k = keyBytes.iterator();
                while(k.hasNext()) {
                    byte[] key = k.next();
                    Key pk = new Key((Persistit)null);
                    KeyState ks = new KeyState(key);
                    pk.clear();
                    ks.copyTo(pk);
//                    System.out.println("Table = "+utable.getTableName()+" key = "+pk);
                    keyMap.put(ks, candidate);
                    keyQueue.add(ks);
                    
                }
            }
        }
        assert userTables.size() > 0;
    }

    public BitSet getProjection() {
        return projection;
    }

    public Collection<RowDef> getUserTables() {
        return userTables.values();
    }

    public Tree<TableDescriptor> getHierarchy() {
        return hierarchy;
    }

    @Override
    public void close() {
        assert false;
    }

    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception {
        assert hasMore == true;

        int chunkDepth = 0;
        RowData row = new RowData();
        boolean scannedARow = false;
        Key pkey = new Key((Persistit)null);
        pkey.clear();
        
        for (; rowIndex < totalRows; rowIndex++) {

            KeyState nextKey = keyQueue.poll();
            assert nextKey != null;
            List<FieldArray> fields = keyMap.get(nextKey).getFieldArrayList();
            RowDef rowDef = userTables.get(keyMap.get(nextKey).getTableId());
            
            int nextRowSize = 0;
            for (int i = 0; i < fields.size(); i++) {
                nextRowSize += fields.get(i).getNextFieldSize();
            }
            nextRowSize += RowData.MINIMUM_RECORD_LENGTH+(rowDef.getFieldCount() % 8 == 0 ?
                    rowDef.getFieldCount()/8 : rowDef.getFieldCount()/8+1);
            assert nextRowSize > 0;
            
            if (nextRowSize > payload.limit() - payload.position()) {
                keyQueue.add(nextKey);
                break;
            } else {
                pkey.clear();
                nextKey.copyTo(pkey);
                /*System.out.println("VCollector: table = "
                        +userTables.get(keyMap.get(nextKey).getTableId()).getTableName()
                        +", key = "+pkey
                        +", chunkDepth = "+chunkDepth+", nextRowSize = "
                        +nextRowSize+" fields ="+fields.size());
                */
                row.reset(payload.array(), chunkDepth, nextRowSize);
                row.mergeFields(userTables.get(keyMap.get(nextKey).getTableId()), fields, nullMap);
                keyMap.get(nextKey).incrementCursor();
                scannedARow = true;
                chunkDepth += nextRowSize;
            }
        }

        if (rowIndex == totalRows) {
            hasMore = false;
        }

        assert hasMore || scannedARow;
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
    
    private BitSet projection;
    private BitSet nullMap;
    private Tree<TableDescriptor> hierarchy;
    
    TreeMap<KeyState, TableDescriptor> keyMap;
    PriorityQueue<KeyState> keyQueue;
    private int totalRows;

//    private int rowSize;
//    private int rawDataSize;
    private int rowIndex;
//    private int rows;
    private int fields;
    private RowDef table;
    private TreeMap<Integer, RowDef> userTables;
    // private ArrayList<FieldArray> fields;
    // private ColumnMapper columnMapper;

}
