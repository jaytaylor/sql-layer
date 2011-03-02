/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

/**
 * 
 */
package com.akiban.server.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.server.IndexDef;
import com.akiban.server.RowData;
import com.akiban.server.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.util.Tap;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;

public class PersistitStoreRowCollector implements RowCollector {

    static final Logger LOG = LoggerFactory.getLogger(PersistitStoreRowCollector.class
            .getName());

    private static final Tap SCAN_NEXT_ROW_TAP = Tap.add("read: next_row");
    
    private static final int MAX_SHORT_RECORD = 4096;

    private static final AtomicLong counter = new AtomicLong();

    private long id;

    private final PersistitStore store;
    
    private final Session session;

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

    // true: scan table
    // false: scan index
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

    private RowTransport transport;

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

    PersistitStoreRowCollector(Session session,
                               PersistitStore store,
                               int scanFlags,
                               RowData start,
                               RowData end,
                               byte[] columnBitMap,
                               RowDef rowDef,
                               int indexId)
        throws PersistitException
    {
        this.id = counter.incrementAndGet();
        this.store = store;
        this.session = session;
        this.columnBitMap = columnBitMap;
        this.scanFlags = scanFlags;
        this.columnBitMapOffset = rowDef.getColumnOffset();
        this.columnBitMapWidth = rowDef.getFieldCount();
        this.rowDefId = rowDef.getRowDefId();

        if (rowDef.isGroupTable()) {
            this.groupRowDef = rowDef;
        } else {
            this.groupRowDef = store.getRowDefCache().getRowDef(rowDef.getGroupRowDefId());
        }

        this.projectedRowDefs = computeProjectedRowDefs(rowDef, this.groupRowDef, columnBitMap);

        if (this.projectedRowDefs.length == 0) {
            this.more = false;
        } else {
            Key.EdgeValue edge;
            if (isAscending()) {
                this.direction = isLeftInclusive() ? Key.GTEQ : Key.GT;
                edge = Key.BEFORE;
            } else {
                this.direction = isRightInclusive() && !isPrefixMode() ? Key.LTEQ : Key.LT;
                edge = Key.AFTER;
            }
            this.pendingRowData = new RowData[this.projectedRowDefs.length];
            this.hEx = store.getExchange(session, rowDef, null);
            this.hFilter = computeHFilter(rowDef, start, end);
            this.hEx.append(edge);
            this.lastKey = new Key(hEx.getKey());

            if (indexId != 0) {
                final IndexDef def = rowDef.getIndexDef(indexId);
                // Don't use the primary key index for ROOT tables - the
                // index tree is not populated because it is redundant
                // with the h-tree itself.
                if (!def.isHKeyEquivalent()) {
                    this.indexDef = def;
                    if (isPrefixMode()) {
                        prefixModeIndexField = rowDef.getColumnOffset() + def.getFields()[def.getFields().length - 1];
                    }
                    this.iEx = store.getExchange(session, rowDef, indexDef).append(edge);
                    this.iFilter = computeIFilter(indexDef, rowDef, start, end);
/* TODO: disabled due to bugs 687212, 687213
                    coveringFields = computeCoveringIndexFields(rowDef, def, columnBitMap);
*/

                    if (store.isVerbose() && LOG.isInfoEnabled()) {
                        LOG.info("Select using index " + indexDef + " filter="
                                 + iFilter
                                 + (coveringFields != null ? " covering" : ""));
                    }
                }
            }

            for (int level = 0; level < pendingRowData.length; level++) {
                pendingRowData[level] = new RowData(new byte[PersistitStore.INITIAL_BUFFER_SIZE]);
            }

            this.pendingFromLevel = 0;
            this.pendingToLevel = 0;
            this.transport = new MessageRowTransport();
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Starting Scan on rowDef=" + rowDef.toString() + ": leafRowDefId=" + leafRowDefId);
        }
    }

    public void outputToMessage(boolean outputToMessage)
    {
        if (outputToMessage) {
            // Leave transport alone
            assert transport instanceof MessageRowTransport;
        } else {
            if (transport instanceof MessageRowTransport) {
                transport = new RowDataRowTransport();
            }
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
    KeyFilter computeHFilter(final RowDef rowDef, final RowData start, final RowData end)
    {
        RowDef leafRowDef = projectedRowDefs[projectedRowDefs.length - 1];
        KeyFilter.Term[] terms = new KeyFilter.Term[leafRowDef.getHKeyDepth()];
        Key key = hEx.getKey();
        HKey hKey =
            rowDef.isGroupTable()
            ? leafRowDef.userTable().branchHKey()
            : leafRowDef.userTable().hKey();
        int t = 0;
        List<HKeySegment> segments = hKey.segments();
        for (int s = 0; s < segments.size(); s++) {
            HKeySegment hKeySegment = segments.get(s);
            RowDef def = store.getRowDefCache().getRowDef(hKeySegment.table().getTableId());
            key.clear().reset().append(def.getOrdinal()).append(def.getOrdinal());
            // using termFromKeySegments avoids allocating a new Key object
            terms[t++] = KeyFilter.termFromKeySegments(key, key, true, true);
            List<HKeyColumn> segmentColumns = hKeySegment.columns();
            for (int c = 0; c < segmentColumns.size(); c++) {
                HKeyColumn segmentColumn = segmentColumns.get(c);
                KeyFilter.Term filterTerm;
                // A group table row has columns that are constrained to be equals, e.g. customer$cid and order$cid.
                // The non-null values in start/end could restrict one or the other, but the hkey references one
                // or the other. For the current segment column, use a literal for any of the equivalent columns.
                // For a user table, segmentColumn.equivalentColumns() == segmentColumn.column().
                filterTerm = KeyFilter.ALL;
                // Must end loop as soon as term other than ALL is found because computeKeyFilterTerm has
                // side effects if it returns anything else.
                List<Column> matchingColumns = segmentColumn.equivalentColumns();
                for (int m = 0; filterTerm == KeyFilter.ALL && m < matchingColumns.size(); m++) {
                    Column column = matchingColumns.get(m);
                    filterTerm = computeKeyFilterTerm(key, rowDef, start, end, column.getPosition());
                }
                terms[t++] = filterTerm;
            }
        }
        key.clear();
        return new KeyFilter(terms, 0, isDeepMode() ? Integer.MAX_VALUE : terms.length);
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
        final IndexDef.H2I[] h2iArray = indexDef.indexKeyFields();
        final List<I2R> coveredFields = new ArrayList<I2R>();
        for (int fieldIndex = 0; fieldIndex < rowDef.getFieldCount(); fieldIndex++) {
            if (isBit(columnBitMap, fieldIndex)) {
                boolean found = false;
                for (int j = 0; j < h2iArray.length; j++) {
                    if (h2iArray[j].fieldIndex() == fieldIndex) {
                        found = true;
                        coveredFields.add(new I2R(j, h2iArray[j].fieldIndex()));
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
     * Selects and returns the next row in tree-traversal. As a side-effect, this method
     * affects the value of <tt>more</tt> to indicate whether there are
     * additional rows in the tree; {@link #hasMore()} returns this value.
     * Returns null if there are no more rows.
     */
    @Override
    public RowData collectNextRow() throws Exception
    {
        RowDataRowTransport rowDataRowTransport = (RowDataRowTransport) transport;
        rowDataRowTransport.clear();
        sendNextRowToTransport();
        return rowDataRowTransport.rowData();
    }

    /**
     * Selects the next row in tree-traversal order and puts it into the payload
     * ByteBuffer if there is room. Returns <tt>true</tt> if and only if a row
     * was added to the payload ByteBuffer. As a side-effect, this method
     * affects the value of <tt>more</tt> to indicate whether there are
     * additional rows in the tree; {@link #hasMore()} returns this value.
     *
     * Return value is true if hasMore() is false or hasMore() is true but the next row doesn't
     * fit in payload.
     */
    @Override
    public boolean collectNextRow(ByteBuffer payload) throws Exception
    {
        ((MessageRowTransport) transport).payload(payload);
        int available = payload.limit() - payload.position();
        if (available < 4) {
            throw new IllegalStateException(
                String.format("Payload byte buffer must have at least 4 available bytes: %s", available));
        }
        return sendNextRowToTransport();
    }

    private boolean sendNextRowToTransport() throws Exception
    {
        if (!more) {
            transport.noMoreRows();
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

                if (transport.deliverRow()) {
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
                        // TODO: ORPHANS - Don't assume the row with the current key actually exists.
                        prepareRow(hEx, level);
                        if (level + 1 < pendingRowData.length) {
                            pendingRowData[level + 1].differsFromPredecessorAtKeySegment(rowDef.getHKeyDepth());
                        }
                        hKey.copyTo(lastKey);
                        hKey.setEncodedSize(keySize);

                        if (level < pendingFromLevel) {
                            pendingFromLevel = level;
                        }
                        if (level >= pendingToLevel) {
                            pendingToLevel = level + 1;
                        }
                    }
                }
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
                if (isDeepMode() && depth > projectedRowDefs[projectedRowDefs.length - 1].getHKeyDepth()) {
                    // Current row's type is deeper that the last projectedRowDef. E.g., projectedRowDefs
                    // contains [Customer, Order] and the current row is an Item. The last slot in pendingRowData
                    // gets reused to store the row and should then be immediately delivered on the next pass through
                    // the loop.
                    int level = pendingRowData.length - 1;
                    prepareRow(hEx, level);
                    if (level < pendingFromLevel) {
                        pendingFromLevel = level;
                    }
                    hKey.copyTo(lastKey);
                    pendingToLevel = level + 1;
                } else {
                    // Current row's type is one of the projectedRowDefs.
                    // TODO: ORPHANS - below depth, set deeper rows to all null.
                    for (int level = projectedRowDefs.length; --level >= 0;) {
                        if (depth == projectedRowDefs[level].getHKeyDepth()) {
                            prepareRow(hEx, level);
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

    void prepareRow(final Exchange exchange, final int level)
            throws Exception
    {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Preparing row at " + exchange);
        }
        store.expandRowData(exchange, pendingRowData[level]);
        int differsAtKeySegment = exchange.getKey().firstUniqueSegmentDepth(lastKey);
        pendingRowData[level].differsFromPredecessorAtKeySegment(differsAtKeySegment);
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

    boolean isAscending() {
        return (scanFlags & SCAN_FLAGS_DESCENDING) == 0;
    }

    boolean isLeftInclusive() {
        return (scanFlags & SCAN_FLAGS_START_EXCLUSIVE) == 0;
    }

    boolean isRightInclusive() {
        return (scanFlags & SCAN_FLAGS_END_EXCLUSIVE) == 0;
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
    public boolean hasMore() {
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
            store.releaseExchange(session, hEx);
            hEx = null;
        }
        if (iEx != null) {
            store.releaseExchange(session, iEx);
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

    private interface RowTransport
    {
        boolean deliverRow();
        void noMoreRows();
    }

    private class MessageRowTransport implements RowTransport
    {
        @Override
        public boolean deliverRow()
        {
            RowData rowData = pendingRowData[pendingFromLevel];
            boolean isLeaf = pendingFromLevel + 1 == pendingRowData.length;
            if (rowData.getRowSize() + 4 < payload.limit() - payload.position()) {
                int position = payload.position();
                payload.put(rowData.getBytes(), rowData.getRowStart(), rowData.getRowSize());
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
                repeatedRows += pendingFromLevel - (almostDeliveredRows - deliveredRows);
                pendingFromLevel = 0;
                // Trim off any non-leaf rows at end of buffer
                payload.reset();
                almostDeliveredRows = deliveredRows;
                return false;
            }
        }

        @Override
        public void noMoreRows()
        {
            deliveredBytes += payload.position();
            deliveredBuffers++;
        }

        void payload(ByteBuffer payload)
        {
            this.payload = payload;
        }

        private ByteBuffer payload;
    }

    private class RowDataRowTransport implements RowTransport
    {
        @Override
        public boolean deliverRow()
        {
            rowData = pendingRowData[pendingFromLevel].copy();
            return true;
        }

        @Override
        public void noMoreRows()
        {
        }

        RowData rowData()
        {
            return rowData;
        }

        void clear()
        {
            rowData = null;
        }

        private RowData rowData;
    }
}
