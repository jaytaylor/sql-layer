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
import java.util.*;

public class VCollector implements RowCollector {

    public VCollector(VMeta meta, final RowDefCache rowDefCache,
            final int rowDefId, final byte[] columnBitMap) throws IOException {        
        assert columnBitMap != null;
        assert meta != null;
        
        hasMore = true;
        rowSize = 0;
        rawDataSize = 0;
        fields = 0;
        userTables = null;

        table = rowDefCache.getRowDef(rowDefId);
        
        projection = new BitSet(table.getFieldCount());
        nullMap = new BitSet(table.getFieldCount());
        columnMapper = new ColumnMapper();
        
        assert table != null;
        projection.clear();
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

        ArrayList<Tree<Table>> nodes = new ArrayList<Tree<Table>>();
        ArrayList<Tree<Table>> rootCandidates = new ArrayList<Tree<Table>>();
        
        userTables = new ArrayList<RowDef>();
        for (int i = 0; i < table.getUserTableRowDefs().length; i++) {
            
            final RowDef utable = table.getUserTableRowDefs()[i];
            Tree<Table> node = new Tree<Table>(new Table(utable.getParentRowDefId(), 
                    utable.getRowDefId()));
            
            int offset = utable.getColumnOffset();
            int distance = offset + utable.getFieldCount();
            assert distance <= table.getFieldCount();
            for (int j = offset; j < distance; j++) {
                if (projection.get(j)) {
                    userTables.add(utable);
                    ColumnDescriptor cdes = meta.lookup(table.getRowDefId(), j);
                    assert cdes != null;
                    assert node.getNode() != null;
                    node.getNode().add(cdes);
                    columnMapper.add(cdes);
                    rawDataSize += cdes.getFieldSize();
                }
            }
 
            rootCandidates.add(node);
            Iterator<Tree<Table>> j = nodes.iterator();
            while(j.hasNext()) {
                Tree<Table> t = j.next();
                if(t.getNode().getRoot()  == node.getNode().getTableId()) {
                    node.add(t);
                    assert rootCandidates.remove(t);
                } else if(node.getNode().getRoot() == t.getNode().getTableId()) {
                    t.add(node);
                    assert rootCandidates.remove(node);
               }
            }
            nodes.add(node);
        }
        //System.out.println("nodes = "+ nodes);
        //System.out.println("rootCandidates = "+rootCandidates);
        assert rootCandidates.size() == 1;
        hierarchy = rootCandidates.get(0);
        assert hierarchy != null;
        // XXX - this is because the null map requires 1 byte per 8 fields.
        // this needs to be improved in the RowData/RowDef --
        // we should not be calculating it; it should be returned by the row
        // data.
        rowSize = rawDataSize + RowData.MINIMUM_RECORD_LENGTH
                + (table.getFieldCount() % 8 == 0 ? table.getFieldCount() / 8
                        : table.getFieldCount() / 8 + 1);
        assert userTables.size() > 0;
    }
    
    public BitSet getProjection() {
        return projection;
    }

    public ArrayList<RowDef> getUserTables() {
        return userTables;
    }

    public Tree<Table> getHierarchy() {
        return hierarchy;
    }
    
    @Override
    public void close() {
        assert false;
    }

    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception {
        assert hasMore == true;
        int count = 0;
        int chunkSize = payload.limit() - payload.position();
        assert chunkSize > 0;

        ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
        int numRows = chunkSize / rowSize;
        // computeHKeys(numRows);
        // compute
        ColumnMapper.MapInfo ret = columnMapper.mapChunk(buffers,  
                chunkSize - ((rowSize - rawDataSize)*numRows));

        if (!ret.more) {
            assert ret.bytes % rawDataSize == 0;
            numRows = (int)ret.bytes/rawDataSize;
            columnMapper.close();
            // XXX - this is badness.
            hasMore = false;
        }

        for (int i = 0; i < numRows; i++) {
            RowData newRow = new RowData(payload.array(), ((int)i)*rowSize,
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
    private int rawDataSize;
    private int fields;
    private RowDef table;
    private ArrayList<RowDef> userTables;
    private ColumnMapper columnMapper;
    private BitSet projection;
    private BitSet nullMap;
    private Tree<Table> hierarchy;
}
