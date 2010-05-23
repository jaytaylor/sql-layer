/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 *
 */

import java.nio.ByteBuffer;
import com.akiban.vstore.*;
import com.akiban.cserver.*;
import java.util.*;

public class VCollector implements RowCollector {

    public VCollector(VMeta meta, final RowDefCache rowDefCache,
            final int rowDefId, final byte[] columnBitMap) {

        assert columnBitMap != null;
        hasMore = false;
        rowSize = 0;
//        totalBytes = 0;
        fields = 0;
        columnMapper = null;
        userTables = null;

        table = rowDefCache.getRowDef(rowDefId);
        projection = new BitSet(table.getFieldCount());
        projection.clear();
        nullMap = new BitSet(table.getFieldCount());
        nullMap.clear();
        
        for (int i = 0; i < table.getFieldCount(); i++) {
            if ((columnBitMap[i / 8] & (1 << (i % 8))) != 0) {
                projection.set(i, true);
                fields++;
            } else {
                nullMap.set(i, true);
            }
        }

        if (!table.isGroupTable()) {
            table = rowDefCache.getRowDef(table.getGroupRowDefId());
        }

        userTables = new ArrayList<RowDef>();
        for (int i = 0; i < table.getUserTableRowDefs().length; i++) {
            final RowDef utable = table.getUserTableRowDefs()[i];
            int offset = utable.getColumnOffset();
            int distance = offset + utable.getFieldCount();
            assert distance <= table.getFieldCount();
            for (int j = offset; j < distance; j++) {
                if (projection.get(j)) {
                    userTables.add(utable);
                }
            }
        }
        assert userTables.size() > 0;
        columnMapper = new ColumnMapper();
    }

    public BitSet getProjection() {
        return projection;
    }

    public ArrayList<RowDef> getUserTables() {
        return userTables;
    }

    // XXX - hack for testing purposes
    public void setColumnDescriptors(List<ColumnDescriptor> theColumns)
            throws Exception {
        assert theColumns.size() > 0;
        columns = theColumns;
        int rowCount = (int) columns.iterator().next().getFieldCount();
        Iterator<ColumnDescriptor> i = columns.iterator();
        rowSize = 0;

        while (i.hasNext()) {
            ColumnDescriptor cdes = i.next();
            columnMapper.add(cdes);
            rowSize += cdes.getFieldSize();
            assert rowCount == cdes.getFieldCount();
        }
        // XXX - this is because the null map requires 1 byte per 8 fields.
        // this needs to be improved in the RowData/RowDef --
        // we should not be calculating it; it should be returned by the row
        // data.
        rowSize += RowData.MINIMUM_RECORD_LENGTH
                + (table.getFieldCount() % 8 == 0 ? table.getFieldCount() / 8 : table.getFieldCount() / 8 + 1);
//        totalBytes = rowCount * (long) rowSize;
    }

    @Override
    public void close() {
        assert false;
    }

    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception {

        int chunkSize = payload.limit() - payload.position();
        assert chunkSize > 0;

        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        int numRows = chunkSize / rowSize;
        boolean done = columnMapper.mapChunk(buffers, chunkSize);
        assert buffers.size() == columns.size();

        if (done) {
            int size = buffers.get(0).capacity();
            assert size >= columns.get(0).getFieldSize();
            numRows = size / columns.get(0).getFieldSize();
            assert numRows > 0;
            columnMapper.close();
            // XXX - this is badness.
            hasMore = false;
        }

        for (int i = 0; i < numRows; i++) {
            RowData newRow = new RowData(payload.array(), ((int) i) * rowSize,
                    rowSize);
            newRow.mergeFields(table, buffers, i, nullMap);
        }
        return true;
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
    private int rowSize;
    private int fields;
    //private long totalBytes;
    private RowDef table;
    private ArrayList<RowDef> userTables;
    private ColumnMapper columnMapper;
    private List<ColumnDescriptor> columns;
    private BitSet projection;
    private BitSet nullMap;
}
