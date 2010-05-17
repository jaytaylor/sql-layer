/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 *
 */

import java.nio.ByteBuffer;
import java.util.Properties;

import com.akiban.util.*;
import com.akiban.vstore.*;
import com.akiban.cserver.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.*;

public class VCollector implements RowCollector {

    public VCollector(final CServerConfig c, final RowDefCache rowDefCache,
            final int rowDefId, final byte[] columnBitMap) throws Exception {

        try {

            this.columnBitMap = columnBitMap;
            rowDef = rowDefCache.getRowDef(rowDefId);
            columnMapper = new ColumnMapper();
            LOG.info("Vertical scan of table: "
                    + rowDef.toString()
                    + "on columns "
                    + CServerUtil.hex(this.columnBitMap, 0,
                            this.columnBitMap.length));

            if (rowDef.isGroupTable()) {
                this.groupRowDef = rowDef;
            } else {
                this.groupRowDef = rowDefCache.getRowDef(rowDef
                        .getGroupRowDefId());
            }
            // computeProjection();
        } catch (Exception e) {
            LOG.info("exception trying to create the vstore row collector");
            throw e;
        }
    }
    
    // XXX - hack for testing purposes
    public void setColumnDescriptors(List<ColumnDescriptor> theColumns) {
        assert theColumns.size() > 0;
        columns = theColumns;
        long fieldCount = columns.iterator().next().getFieldCount();
        Iterator<ColumnDescriptor> i = columns.iterator();
        rowSize = 0;

        while (i.hasNext()) {
            ColumnDescriptor cdes = i.next();
            columnMapper.add(cdes);
            rowSize += cdes.getFieldSize();
            assert fieldCount == cdes.getFieldCount();
        }
        rowSize += RowData.MINIMUM_RECORD_LENGTH+1;
        totalBytes = fieldCount * (long)rowSize;
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
            System.out.println("chunkSize ="+chunkSize+"totalBytes = "+totalBytes);
            assert chunkSize >= totalBytes;
            int size = buffers.get(0).capacity();
            assert size >= columns.get(0).getFieldSize();
            numRows = size/columns.get(0).getFieldSize();
            assert numRows > 0;
            
            // XXX - this is badness.
            hasMore = false;
        }

        for (int i = 0; i < numRows; i++) {
            RowData newRow = new RowData(payload.array(), ((int)i)*rowSize, rowSize);
            newRow.mergeFields(rowDef, buffers, i);
        }
        return true;
    }

    @Override
    public boolean hasMore() throws Exception {
        //assert false;
        return hasMore;
    }

    @Override
    public void refreshAncestorRows() {
        LOG.error("refreshAncestorRows is not implemented");
        assert false;
    }

    private static final Log LOG = LogFactory
            .getLog(VCollector.class.getName());
    private final RowDef rowDef;
    private final RowDef groupRowDef;
    private final byte[] columnBitMap;
    private boolean hasMore = true;
    private int rowSize=0;
    private long totalBytes=0;
    private ColumnMapper columnMapper = null;
    private List<ColumnDescriptor> columns = null;
}
