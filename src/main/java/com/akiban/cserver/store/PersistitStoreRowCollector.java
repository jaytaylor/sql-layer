/**
 * 
 */
package com.akiban.cserver.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.MySQLErrorConstants;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;

public class PersistitStoreRowCollector implements RowCollector,
        MySQLErrorConstants {

    static final Log LOG = LogFactory.getLog(PersistitStoreRowCollector.class
            .getName());

    private static final Tap SCAN_NEXT_ROW_TAP = Tap.add("read: next_row");
    
    private static final int MAX_SHORT_RECORD = 4096;

    private static final AtomicLong counter = new AtomicLong();

    private long id;

    private final PersistitStore store;

    private final byte[] columnBitMap;

    private final int scanFlags;

    private int columnBitMapOffset;

    private int columnBitMapWidth;

    private int rowDefId;

    private RowDef groupRowDef;

    private IndexDef indexDef;

    private int leafRowDefId;

    private KeyFilter iFilter;

    private KeyFilter hFilter;

    private boolean more = true;

    private RowDef[] projectedRowDefs;

    private RowData[] pendingRowData;

    private int pendingFromLevel;

    private int pendingToLevel;

    private int indexPinnedKeySize;

    private boolean traverseMode;

    private int prefixModeIndexField = -1;

    private Exchange iEx;

    private Exchange hEx;

    private Key lastKey;

    private Key.Direction direction;

    private I2R[] coveringFields;

    private int deliveredRows;

    private int almostDeliveredRows;

    private int deliveredBuffers;

    private long deliveredBytes;

    private int repeatedRows;

    /**
     * Structure that maps the index key field depth to a RowData's field index.
     * Used to supporting covering index.
     */
    static class I2R {

        private final int indexKeyDepth;

        private final int rowDataFieldIndex;

        private I2R(final int indexKeyDepth, final int rowDataFieldIndex) {
            this.indexKeyDepth = indexKeyDepth;
            this.rowDataFieldIndex = rowDataFieldIndex;
        }

        public int getIndexKeyDepth() {
            return indexKeyDepth;
        }

        public int getRowDataFieldIndex() {
            return rowDataFieldIndex;
        }

        public String toString() {
            return "I2R(indexDepth=" + getIndexKeyDepth() + ",fieldIndex="
                    + getRowDataFieldIndex() + ")";
        }
    }

    PersistitStoreRowCollector(PersistitStore store, final int scanFlags,
            final RowData start, final RowData end, final byte[] columnBitMap,
            RowDef rowDef, final int indexId) throws Exception {
        this.id = counter.incrementAndGet();
        this.store = store;
        this.columnBitMap = columnBitMap;
        this.scanFlags = scanFlags;
        this.columnBitMapOffset = rowDef.getColumnOffset();
        this.columnBitMapWidth = rowDef.getFieldCount();
        this.rowDefId = rowDef.getRowDefId();

        if (rowDef.isGroupTable()) {
            this.groupRowDef = rowDef;
        } else {
            this.groupRowDef = store.getRowDefCache().getRowDef(
                    rowDef.getGroupRowDefId());
        }

        this.projectedRowDefs = computeProjectedRowDefs(rowDef,
                this.groupRowDef, columnBitMap);

        if (this.projectedRowDefs.length == 0) {
            this.more = false;
        } else {
            this.pendingRowData = new RowData[this.projectedRowDefs.length];
            this.hEx = store.getExchange(rowDef, null);
            this.hFilter = computeHFilter(rowDef, start, end);
            this.lastKey = new Key(hEx.getKey());

            if (indexId != 0) {
                final IndexDef def = rowDef.getIndexDef(indexId);
                // Don't use the primary key index for ROOT tables - the
                // index tree is not populated because it is redundant
                // with the h-tree itself.
                if (!def.isHKeyEquivalent()) {
                    this.indexDef = def;
                    if (isPrefixMode()) {
                        prefixModeIndexField = rowDef.getColumnOffset()
                                + def.getFields()[def.getFields().length - 1];
                    }
                    this.iEx = store.getExchange(rowDef, indexDef);
                    this.iFilter = computeIFilter(indexDef, rowDef, start, end);
                    coveringFields = computeCoveringIndexFields(rowDef, def,
                            columnBitMap);
                    
                    if (store.isVerbose() && LOG.isInfoEnabled()) {
                        LOG.info("Select using index " + indexDef + " filter="
                                + iFilter
                                + (coveringFields != null ? " covering" : ""));
                    }
                }
            }

            for (int level = 0; level < pendingRowData.length; level++) {
                pendingRowData[level] = new RowData(
                        new byte[PersistitStore.INITIAL_BUFFER_SIZE]);
            }

            this.pendingFromLevel = 0;
            this.pendingToLevel = 0;
            this.direction = isAscending() ? (isLeftInclusive() ? Key.GTEQ
                    : Key.GT)
                    : (isRightInclusive() && !isPrefixMode() ? Key.LTEQ
                            : Key.LT);
        }

        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting Scan on rowDef=" + rowDef.toString()
                    + ": leafRowDefId=" + leafRowDefId);
        }
    }

    RowDef[] computeProjectedRowDefs(final RowDef rowDef,
            final RowDef groupRowDef, final byte[] columnBitMap) {
        final int columnOffset = rowDef.getColumnOffset();
        final int columnCount = rowDef.getFieldCount();

        boolean isEmpty = true;
        for (int index = 0; index < columnBitMap.length; index++) {
            if (columnBitMap[index] != 0) {
                isEmpty = false;
                break;
            }
        }
        //
        // Handles special case of SELECT COUNT(*)
        // In the special-special case that this is a one-table group,
        // project onto that table. Otherwise, if this is a group table,
        // we can't infer what the projection should be so we throw
        // an IllegalStateException.
        //
        if (isEmpty) {

            if (rowDef.isGroupTable()) {
                if (rowDef.getUserTableRowDefs().length == 1) {
                    return rowDef.getUserTableRowDefs();
                } else {
                    throw new IllegalStateException(
                            "Attempt to SELECT COUNT(*) on a group table");
                }
            } else {
                return new RowDef[] { rowDef };
            }
        }

        List<RowDef> projectedRowDefList = new ArrayList<RowDef>();
        for (int index = 0; index < groupRowDef.getUserTableRowDefs().length; index++) {
            final RowDef def = groupRowDef.getUserTableRowDefs()[index];

            int from = def.getColumnOffset();
            int width = def.getFieldCount();
            from -= columnOffset;
            if (from < 0) {
                width += from;
                from = 0;
            }
            if (width > columnCount) {
                width = columnCount;
            }
            if (from + width > columnBitMap.length * 8) {
                width = columnBitMap.length * 8 - from;
            }

            boolean projected = false;
            for (int bit = from; !projected && bit < from + width; bit++) {
                if (isBit(columnBitMap, bit)) {
                    projected = true;
                }
            }
            if (projected) {
                projectedRowDefList.add(def);
            }
        }
        return projectedRowDefList.toArray(new RowDef[projectedRowDefList
                .size()]);
    }

    /**
     * Construct a KeyFilter on the h-key. This minimally contains a template
     * for the ordinal tree identifiers. If any of the supplied index range
     * values pertain to primary key fields, these will also be constrained.
     * 
     * @param rowDef
     * @param start
     * @param end
     * @return
     * @throws Exception
     */
    KeyFilter computeHFilter(final RowDef rowDef, final RowData start,
            final RowData end) throws Exception {
        final RowDef leafRowDef = projectedRowDefs[projectedRowDefs.length - 1];
        final KeyFilter.Term[] terms = new KeyFilter.Term[leafRowDef
                .getHKeyDepth()];
        final Key key = hEx.getKey();
        int index = terms.length;
        RowDef def = leafRowDef;

        while (def != null) {
            final int[] fields = def.getPkFields();
            if (index < (fields.length + 1)) {
                throw new IllegalStateException(
                        "Length mismatch in computeHFilter: def=" + def
                                + " leafRowDef=" + leafRowDef + " index="
                                + index);
            }
            terms[index - fields.length - 1] = KeyFilter.simpleTerm(def
                    .getOrdinal());
            for (int k = 0; k < fields.length; k++) {
                terms[index - fields.length + k] = computeKeyFilterTerm(key,
                        rowDef, start, end, fields[k] + def.getColumnOffset()
                                - rowDef.getColumnOffset());
            }
            index -= (fields.length + 1);
            final int parentId = def.getParentRowDefId();
            def = parentId == 0 ? null : store.getRowDefCache().getRowDef(
                    parentId);
        }
        if (index != 0) {
            throw new IllegalStateException(
                    "Length mismatch in computeHFilter: leafRowDef="
                            + leafRowDef + " index=" + index);
        }
        key.clear();
        return new KeyFilter(terms, 0, isDeepMode() ? Integer.MAX_VALUE
                : terms.length);
    }

    /**
     * Construct a key filter on the index key
     * 
     * @param indexDef
     * @param rowDef
     * @param start
     * @param end
     * @return
     */
    KeyFilter computeIFilter(final IndexDef indexDef, final RowDef rowDef,
            final RowData start, final RowData end) {
        final Key key = iEx.getKey();
        final int[] fields = indexDef.getFields();
        final KeyFilter.Term[] terms = new KeyFilter.Term[fields.length];
        for (int index = 0; index < fields.length; index++) {
            terms[index] = computeKeyFilterTerm(key, rowDef, start, end,
                    fields[index]);
        }
        key.clear();
        return new KeyFilter(terms, terms.length, Integer.MAX_VALUE);
    }

    /**
     * Returns a KeyFilter term if the specified field of either the start or
     * end RowData is non-null, else null.
     * 
     * @param key
     * @param rowDef
     * @param start
     * @param end
     * @param fieldIndex
     * @return
     */
    private KeyFilter.Term computeKeyFilterTerm(final Key key,
            final RowDef rowDef, final RowData start, final RowData end,
            final int fieldIndex) {
        if (fieldIndex < 0 || fieldIndex >= rowDef.getFieldCount()) {
            return KeyFilter.ALL;
        }
        final long lowLoc = start == null ? 0 : rowDef.fieldLocation(start,
                fieldIndex);
        final long highLoc = end == null ? 0 : rowDef.fieldLocation(end,
                fieldIndex);
        if (lowLoc != 0 || highLoc != 0) {
            key.clear();
            key.reset();
            if (lowLoc != 0) {
                store.appendKeyField(key, rowDef.getFieldDef(fieldIndex), start);
            } else {
                key.append(Key.BEFORE);
                key.setEncodedSize(key.getEncodedSize() + 1);
            }
            if (highLoc != 0) {
                store.appendKeyField(key, rowDef.getFieldDef(fieldIndex), end);
                if (fieldIndex + rowDef.getColumnOffset() == prefixModeIndexField) {
                    advanceKeyForPrefixMode(key);
                }
            } else {
                key.append(Key.AFTER);
            }
            //
            // Tricky: termFromKeySegments reads successive key segments when
            // called this way.
            //
            return KeyFilter.termFromKeySegments(key, key, true, true);
        } else {
            return KeyFilter.ALL;
        }
    }

    void advanceKeyForPrefixMode(final Key key) {
        int size = key.getEncodedSize();
        final byte[] bytes = key.getEncodedBytes();
        for (int index = size - 1; --index >= 0;) {
            if (bytes[index] == 0) {
                throw new IllegalStateException(
                        "Can't find advancement byte in " + key);
            }
            if (bytes[index] != (byte) 0xFF) {
                bytes[index] = (byte) ((bytes[index] & 0xFF) + 1);
                bytes[index + 1] = 0;
                key.setEncodedSize(index + 1);
                break;
            }
        }
    }

    /**
     * Test for bit set in a column bit map
     * 
     * @param columnBitMap
     * @param column
     * @return <tt>true</tt> if the bit for the specified column in the bit map
     *         is 1
     */
    boolean isBit(final byte[] columnBitMap, final int column) {
        if (columnBitMap == null) {
            if (LOG.isErrorEnabled()) {
                LOG.error("ColumnBitMap is null in ScanRowsRequest on table "
                        + groupRowDef);
            }
            return false;
        }
        if ((column / 8) >= columnBitMap.length || column < 0) {
            if (LOG.isErrorEnabled()) {
                LOG.error("ColumnBitMap is too short in "
                        + "ScanRowsRequest on table " + groupRowDef
                        + " columnBitMap has " + columnBitMap.length
                        + " bytes, but isBit is " + "trying to test bit "
                        + column);
            }
            return false;
        }
        return (columnBitMap[column / 8] & (1 << (column % 8))) != 0;
    }

    /**
     * Determine whether all of the fields specified in the columnBitMap are
     * covered by index key fields, and if so, return an array
     * 
     * @param rowDef
     * @param indexDef
     * @param columnBitMap
     * @return
     */
    I2R[] computeCoveringIndexFields(final RowDef rowDef,
            final IndexDef indexDef, final byte[] columnBitMap) {
        if (rowDef.isGroupTable()) {
            // For now, only works on user tables
            return null;
        }
        final IndexDef.H2I[] h2iArray = indexDef.getIndexKeyFields();
        final List<I2R> coveredFields = new ArrayList<I2R>();
        for (int fieldIndex = 0; fieldIndex < rowDef.getFieldCount(); fieldIndex++) {
            if (isBit(columnBitMap, fieldIndex)) {
                boolean found = false;
                for (int j = 0; j < h2iArray.length; j++) {
                    if (h2iArray[j].getFieldIndex() == fieldIndex) {
                        found = true;
                        coveredFields.add(new I2R(j, h2iArray[j]
                                .getFieldIndex()));
                        break;
                    }
                }
                if (!found) {
                    return null;
                }
            }
        }
        return coveredFields.toArray(new I2R[coveredFields.size()]);
    }

    /**
     * Selects the next row in tree-traversal order and puts it into the payload
     * ByteBuffer if there is room. Returns <tt>true</tt> if and only if a row
     * was added to the payload ByteBuffer. As a side-effect, this method
     * affects the value of <tt>more</tt> to indicate whether there are
     * additional rows in the tree; {@link #hasMore()} returns this value.
     * 
     */
    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception {
        int available = payload.limit() - payload.position();
        if (available < 4) {
            throw new IllegalStateException(
                    "Payload byte buffer must have at least 4 "
                            + "available bytes: " + available);
        }
        if (!more) {
            deliveredBytes += payload.position();
            deliveredBuffers++;
            return false;
        }

        boolean result = false;
        SCAN_NEXT_ROW_TAP.in();

        final Key hKey = hEx.getKey();

        if (indexDef == null) {
            traverseMode = true;
        }

        while (true) {
            //
            // Flush any available pending rows. A row on in the pendingRowData
            // array is available only if the leaf level of the array has been
            // populated.
            //
            if (morePending()) {

                if (deliverRow(pendingRowData[pendingFromLevel], payload,
                        pendingFromLevel + 1 == pendingRowData.length)) {
                    pendingFromLevel++;
                    if (isSingleRowMode() && pendingFromLevel == pendingToLevel) {
                        more = false;
                    }
                    result = true;
                    break;
                } else {
                    result = false;
                    break;
                }
            }

            if (!traverseMode) {
                pendingToLevel = 0;
                //
                // Traverse to next key in Index
                //
                more = iEx.traverse(direction, iFilter, 0);
                if (!more) {
                    //
                    // All done
                    //
                    result = false;
                    break;
                }
                direction = isAscending() ? Key.GT : Key.LT;

                if (coveringFields != null) {
                    prepareCoveredRow(iEx, rowDefId, coveringFields);
                    pendingFromLevel = 0;
                    pendingToLevel = 1;
                    continue;
                }
                store.constructHKeyFromIndexKey(hKey, iEx.getKey(), indexDef);

                final int differsAt = hKey.firstUniqueByteIndex(lastKey);
                final int depth = hKey.getDepth();
                indexPinnedKeySize = hKey.getEncodedSize();

                //
                // Back-fill all needed ancestor rows. An ancestor row is needed
                // if it is in the projection, and if the hKey of the current
                // tree traversal location differs from that of the previously
                // prepared row at a key segment left of the ancestor row's key
                // depth.
                //
                for (int level = 0; level < projectedRowDefs.length; level++) {
                    final RowDef rowDef = projectedRowDefs[level];
                    if (rowDef.getHKeyDepth() > depth) {
                        break;
                    }
                    hKey.indexTo(rowDef.getHKeyDepth());
                    if (differsAt < hKey.getIndex()) {
                        //
                        // This is an ancestor row different from what was
                        // previously delivered so we need to prepare it.
                        //
                        final int keySize = hKey.getEncodedSize();
                        hKey.setEncodedSize(hKey.getIndex());
                        hEx.fetch();
                        prepareRow(hEx, level, rowDef.getRowDefId(),
                                rowDef.getColumnOffset());
                        hKey.setEncodedSize(keySize);

                        if (level < pendingFromLevel) {
                            pendingFromLevel = level;
                        }
                        if (level >= pendingToLevel) {
                            pendingToLevel = level + 1;
                        }
                    }
                }
                hKey.copyTo(lastKey);
                traverseMode = true;
                //
                // Repeat main while-loop to flush any available pending rows
                //
                continue;
            }

            if (traverseMode) {
                //
                // Traverse
                //
                final boolean found = hEx.traverse(direction, hFilter,
                        MAX_SHORT_RECORD);
                direction = isAscending() ? Key.GT : Key.LT;
                
                if (!found
                        || hKey.firstUniqueByteIndex(lastKey) < indexPinnedKeySize) {
                    if (indexDef == null) {
                        more = false;
                        result = false;
                        break;
                    }
                    traverseMode = false;
                    //
                    // To outer while-loop
                    //
                    continue;
                }
                final int depth = hKey.getDepth();
                if (hEx.getValue().getEncodedSize() >= MAX_SHORT_RECORD) {
                    // Get the rest of the value
                    hEx.fetch();
                }
                if (isDeepMode()
                        && depth > projectedRowDefs[projectedRowDefs.length - 1]
                                .getHKeyDepth()) {
                    int level = pendingRowData.length - 1;
                    prepareRow(hEx, level, 0, 0);
                    if (level < pendingFromLevel) {
                        pendingFromLevel = level;
                    }
                    pendingToLevel = level + 1;
                } else {
                    for (int level = projectedRowDefs.length; --level >= 0;) {
                        if (depth == projectedRowDefs[level].getHKeyDepth()) {
                            prepareRow(hEx, level,
                                    projectedRowDefs[level].getRowDefId(),
                                    projectedRowDefs[level].getColumnOffset());
                            hKey.copyTo(lastKey);
                            if (level < pendingFromLevel) {
                                pendingFromLevel = level;
                            }
                            pendingToLevel = level + 1;
                            break;
                        }
                    }
                }
            }
        }
        SCAN_NEXT_ROW_TAP.out();

        return result;
    }

    void prepareRow(final Exchange exchange, final int level,
            final int expectedRowDefId, final int columnOffset)
            throws Exception {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Preparing row at " + exchange);
        }
        store.expandRowData(exchange, expectedRowDefId, pendingRowData[level]);
        //
        // Remove unwanted columns
        //
        if (!isDeepMode()) {
            pendingRowData[level].elide(columnBitMap, columnOffset
                    - columnBitMapOffset, columnBitMapWidth);
        }
    }

    void prepareCoveredRow(final Exchange exchange, final int rowDefId,
            final I2R[] coveringFields) {
        final RowDef rowDef = store.getRowDefCache().getRowDef(rowDefId);
        final RowData rowData = pendingRowData[0];
        final Object[] values = new Object[rowDef.getFieldCount()];
        final Key key = exchange.getKey();
        for (final I2R i2r : coveringFields) {
            key.indexTo(i2r.getIndexKeyDepth());
            values[i2r.getRowDataFieldIndex()] = key.decode();
        }
        rowData.createRow(rowDef, values);
    }

    boolean deliverRow(final RowData rowData, final ByteBuffer payload,
            final boolean isLeaf) throws IOException {
        if (rowData.getRowSize() + 4 < payload.limit() - payload.position()) {
            final int position = payload.position();
            payload.put(rowData.getBytes(), rowData.getRowStart(),
                    rowData.getRowSize());
            almostDeliveredRows++;
            if (isLeaf) {
                payload.mark();
                deliveredRows = almostDeliveredRows;
            }
            if (store.isVerbose() && LOG.isDebugEnabled()) {
                LOG.info("Select row: "
                        + rowData.toString(store.getRowDefCache()) + " len="
                        + rowData.getRowSize() + " position=" + position);
            }
            return true;
        } else {
            // ScanRowsMoreRequest: deliver a full hierarchy.
            repeatedRows += pendingFromLevel
                    - (almostDeliveredRows - deliveredRows);
            pendingFromLevel = 0;
            // Trim off any non-leaf rows at end of buffer
            payload.reset();
            almostDeliveredRows = deliveredRows;
            return false;
        }
    }

    boolean isAscending() {
        return (scanFlags & SCAN_FLAGS_DESCENDING) == 0;
    }

    boolean isLeftInclusive() {
        return (scanFlags & SCAN_FLAGS_START_EXCLUSIVE) == 0;
    }

    boolean isRightInclusive() {
        return (scanFlags & SCAN_FLAGS_START_EXCLUSIVE) == 0;
    }

    boolean isPrefixMode() {
        return (scanFlags & SCAN_FLAGS_PREFIX) != 0;
    }

    boolean isSingleRowMode() {
        return (scanFlags & SCAN_FLAGS_SINGLE_ROW) != 0;
    }

    boolean isDeepMode() {
        return (scanFlags & SCAN_FLAGS_DEEP) != 0;
    }

    boolean morePending() {
        return pendingFromLevel < pendingToLevel
                && pendingToLevel == pendingRowData.length;
    }

    @Override
    public boolean hasMore() throws StoreException {
        if (!more && !morePending()) {
            if (store.isVerbose() && LOG.isInfoEnabled()) {
                LOG.info(String.format("RowCollector %d delivered %,d rows in "
                        + "%,d buffers / %,d bytes", id, deliveredRows,
                        deliveredBuffers + 1, deliveredBytes));
            }
            return false;
        } else {
            return true;
        }
    }

    @Override
    public void close() {
        if (hEx != null) {
            store.releaseExchange(hEx);
            hEx = null;
        }
        if (iEx != null) {
            store.releaseExchange(iEx);
            iEx = null;
        }
    }

    public int getDeliveredRows() {
        return deliveredRows;
    }

    public int getDeliveredBuffers() {
        return deliveredBuffers;
    }

    public long getDeliveredBytes() {
        return deliveredBytes;
    }

    public int getRepeatedRows() {
        return repeatedRows;
    }
    
    public int getTableId() {
        return rowDefId;
    }

    public long getId() {
        return id;
    }

    // Used by indexer
    Exchange getHExchange() {
        return hEx;
    }

    // Used by indexer
    KeyFilter getHFilter() {
        return hFilter;
    }
}