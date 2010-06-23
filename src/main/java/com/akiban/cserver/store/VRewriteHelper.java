/**
 * 
 */
package com.akiban.cserver.store;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;

import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.store.DeltaMonitor.DeltaCursor;
import com.persistit.Key;
import com.persistit.KeyState;
import com.persistit.Persistit;

/**
 * @author percent
 * 
 */
public class VRewriteHelper {

    public VRewriteHelper(VMeta meta, DeltaCursor dc, HashSet<RowDef> tables)
            throws FileNotFoundException, IOException {
        assert meta != null;
        assert dc != null;
        assert tables != null && tables.size() > 0;

        cursor = dc;
        deltaRows = dc.size();

        userTables = new TreeMap<Integer, RowDef>();
        keyQueue = new PriorityQueue<KeyState>();
        keyMap = new TreeMap<KeyState, TableDescriptor>();

        Iterator<RowDef> i = tables.iterator();
        while (i.hasNext()) {
            RowDef table = i.next();
            assert !table.isGroupTable();
            addTable(meta, table);
        }
        totalRows = deltaRows + ondiskRows;
    }

    public int getTotalRows() {
        return totalRows;
    }

    public Delta getNextRow() throws Exception {
        Delta d = null;
        // System.out.println("ondisk index = "+
        // ondiskIndex+", ondiskRows = "+ondiskRows+", deltaindex =" +
        // deltaIndex+", deltaRows ="+ deltaRows);
        if (ondiskIndex < ondiskRows && deltaIndex < deltaRows) {
            KeyState nextKey = keyQueue.peek();
            if (cursor.check(nextKey)) {
                d = nextDeltaRow();
            } else {
                d = nextOndiskRow(nextKey);
            }
        } else if (ondiskIndex < ondiskRows) {
            KeyState nextKey = keyQueue.peek();
            d = nextOndiskRow(nextKey);
        } else if (deltaIndex < deltaRows) {
            d = nextDeltaRow();
        }
        return d;
    }

    private void scanKeys(TableDescriptor tdes) throws IOException {

        List<byte[]> keyBytes = tdes.scanKeys();
        Iterator<byte[]> k = keyBytes.iterator();
        while (k.hasNext()) {
            byte[] key = k.next();
            Key pk = new Key((Persistit) null);
            KeyState ks = new KeyState(key);
            pk.clear();
            ks.copyTo(pk);
            // System.out.println("VCollector: Table = "
            // + utable.getTableName() + " key = " + pk);
            keyMap.put(ks, tdes);
            keyQueue.add(ks);
        }
    }

    private void addTable(VMeta meta, RowDef utable)
            throws FileNotFoundException, IOException {
        IColumnDescriptor kdes = meta.getHKey(utable.getRowDefId());
        if (kdes == null) {
            // if there's no key, then this table is only in the deltas, so
            // we don't need to do anything.
            return;
        }
        TableDescriptor tdes = new TableDescriptor(kdes, utable
                .getParentRowDefId(), utable.getRowDefId());

        for (int j = 0; j < utable.getFieldCount(); j++) {
            // System.out.println("VCollector: " + utable.getTableName()
            // + ", fieldname: " + utable.getFieldDef(k).getName()
            // + ", j = "+ j +", rowDefId = "
            // + utable.getRowDefId() + ", fixedSize = "
            // + utable.getFieldDef(k).isFixedSize());
            IColumnDescriptor cdes = meta.lookup(utable.getRowDefId(), j);
            assert cdes != null;
            assert tdes != null;
            tdes.add(cdes);
        }

        // XXX - h4x0r
        assert tdes.getFieldArrayList().size() > 0;
        ondiskRows += tdes.getRows();
        userTables.put(utable.getRowDefId(), utable);
        scanKeys(tdes);
    }

    private Delta nextOndiskRow(KeyState nextKey) throws IOException {
        List<FieldArray> fields = keyMap.get(nextKey).getFieldArrayList();
        RowDef rowDef = userTables.get(keyMap.get(nextKey).getTableId());

        int nextRowSize = 0;
        for (int i = 0; i < fields.size(); i++) {
            nextRowSize += fields.get(i).getNextFieldSize();
        }
        assert nextRowSize > 0;
        nextRowSize += RowData.MINIMUM_RECORD_LENGTH
                + (rowDef.getFieldCount() % 8 == 0 ? rowDef.getFieldCount() / 8
                        : rowDef.getFieldCount() / 8 + 1);
        byte[] store = new byte[nextRowSize];
        RowData row = new RowData(store);
        BitSet nullmap = new BitSet(rowDef.getFieldCount());
        row.mergeFields(userTables.get(keyMap.get(nextKey).getTableId()),
                fields, nullmap, 0);

        // System.out.println("VCollector: table = "
        // +
        // userTables.get(keyMap.get(nextKey).getTableId()).getTableName()
        // + ", key = " + pkey
        // + ", chunkDepth = " + chunkDepth + ", nextRowSize = "
        // + nextRowSize + " fields =" + fields.size());

        // int k = chunkDepth;
        // while (k < chunkDepth + nextRowSize) {
        // System.out.print(Integer.toHexString(payload.array()[k])+" ");
        // k++;
        // }
        // System.out.println("VCollector decoded row = " +
        // row.toString(cache));
        keyQueue.poll();
        keyMap.get(nextKey).incrementCursor();
        Delta d = new Delta(Delta.Type.Insert, nextKey, rowDef, row);
        ondiskIndex++;
        return d;
    }

    private Delta nextDeltaRow() {
        Delta d = cursor.get();
        if (d != null) {
            cursor.remove();
        }
        deltaIndex++;
        return d;
    }

    private int deltaIndex;
    private int deltaRows;
    private int ondiskIndex;
    private int ondiskRows;
    private int totalRows;
    private DeltaCursor cursor;
    private TreeMap<KeyState, TableDescriptor> keyMap;
    private TreeMap<Integer, RowDef> userTables;
    private PriorityQueue<KeyState> keyQueue;
}
