/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.Store;
import com.foundationdb.util.WrappingByteSource;

import java.util.ArrayList;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_ENTRY_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;

/** Manage index statistics for a Store
 *
 * About index_statistics_entry, single-column and multi-column histograms:
 * - Multi-column histograms were invented first. index_statistics_entry.column_count indicates
 *   the number of columns represented by the entry, e.g. 1 for (a) and 2 for (a, b).
 * - Single-column histograms were added later. The single-column histogram for the leading column
 *   of an index is identical to the multi-column histogram with column_count 1. column_count -2 is
 *   for the second column.
 * So for an index (a, b, c), there are the following column_counts:
 * 1: (a)
 * 2: (a, b)
 * 3: (a, b, c)
 * -2: (b)
 * -3: (c)
 */
public abstract class AbstractStoreIndexStatistics<S extends Store> {
    private final S store;

    public AbstractStoreIndexStatistics(S store) {
        this.store = store;
    }

    protected S getStore() {
        return store;
    }

    public abstract IndexStatistics loadIndexStatistics(Session session, Index index);
    public abstract void removeStatistics(Session session, Index index);
    /** Sample index values and build statistics histograms. */
    public abstract IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime);


    protected long estimateIndexRowCount(Session session, Index index) {
        switch(index.getIndexType()) {
            case TABLE:
            case GROUP:
                return index.leafMostTable().rowDef().getTableStatus().getApproximateRowCount(session);
            case FULL_TEXT:
                throw new UnsupportedOperationException("FullTextIndex row count");
            default:
                throw new IllegalStateException("Unknown index type: " + index);
        }
    }

    protected RowDef getIndexStatsRowDef(Session session) {
        Table table = store.getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        assert (table != null);
        return table.rowDef();
    }

    protected RowDef getIndexStatsEntryRowDef(Session session) {
        Table table = store.getAIS(session).getTable(INDEX_STATISTICS_ENTRY_TABLE_NAME);
        assert (table != null);
        return table.rowDef();
    }


    /* Storage formats.
     * Keep in sync with IndexStatisticsServiceImpl
     */
    private static final int ANALYSIS_TIMESTAMP_FIELD_INDEX = 2;
    private static final int ROW_COUNT_FIELD_INDEX = 3;
    private static final int SAMPLED_COUNT_FIELD_INDEX = 4;

    // Parent keys the same.
    private static final int COLUMN_COUNT_FIELD_INDEX = 2;
    private static final int ITEM_NUMBER_FIELD_INDEX = 3;
    private static final int KEY_STRING_FIELD_INDEX = 4;
    private static final int KEY_BYTES_FIELD_INDEX = 5;
    private static final int EQ_COUNT_FIELD_INDEX = 6;
    private static final int LT_COUNT_FIELD_INDEX = 7;
    private static final int DISTINCT_COUNT_FIELD_INDEX = 8;

    protected final IndexStatistics decodeIndexStatisticsRow(RowData rowData,
                                                             RowDef indexStatisticsRowDef,
                                                             Index index) {
        LegacyRowWrapper row = new LegacyRowWrapper(indexStatisticsRowDef, rowData);

        long analysisTimestamp = ((Integer)row.get(ANALYSIS_TIMESTAMP_FIELD_INDEX)).longValue();
        long rowCount = (Long)row.get(ROW_COUNT_FIELD_INDEX);
        long sampledCount = (Long)row.get(SAMPLED_COUNT_FIELD_INDEX);

        IndexStatistics result = new IndexStatistics(index);
        result.setAnalysisTimestamp(analysisTimestamp * 1000);
        result.setRowCount(rowCount);
        result.setSampledCount(sampledCount);
        return result;
    }

    protected final void decodeIndexStatisticsEntryRow(RowData rowData,
                                                       RowDef indexStatisticsEntryRowDef,
                                                       IndexStatistics indexStatistics) {
        LegacyRowWrapper row = new LegacyRowWrapper(indexStatisticsEntryRowDef, rowData);

        int columnCount = ((Integer)row.get(COLUMN_COUNT_FIELD_INDEX)).intValue();
        int itemNumber = ((Integer)row.get(ITEM_NUMBER_FIELD_INDEX)).intValue();
        String keyString = (String)row.get(KEY_STRING_FIELD_INDEX);

        WrappingByteSource byteSource = (WrappingByteSource)row.get(KEY_BYTES_FIELD_INDEX);
        byte[] keyBytes = byteSource.toByteSubarray();

        long eqCount = (Long)row.get(EQ_COUNT_FIELD_INDEX);
        long ltCount = (Long)row.get(LT_COUNT_FIELD_INDEX);
        long distinctCount = (Long)row.get(DISTINCT_COUNT_FIELD_INDEX);

        int firstColumn = 0; // Correct for multi-column
        if (columnCount < 0) {
            firstColumn = -columnCount - 1;
            columnCount = 1;
        }
        Histogram histogram = indexStatistics.getHistogram(firstColumn, columnCount);
        if (histogram == null) {
            histogram = new Histogram(firstColumn, columnCount, new ArrayList<HistogramEntry>());
            indexStatistics.addHistogram(histogram);
        }
        histogram.getEntries().add(new HistogramEntry(keyString, keyBytes, eqCount, ltCount, distinctCount));
    }

    /** Store statistics into database. */
    public final void storeIndexStatistics(Session session, Index index, IndexStatistics indexStatistics) {
        int tableId = index.leafMostTable().rowDef().getRowDefId();
        RowType indexStatisticsRowType = SchemaCache.globalSchema(index.getAIS()).tableRowType(store.getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME));
        RowType indexStatisticsEntryRowType = SchemaCache.globalSchema(index.getAIS()).tableRowType(store.getAIS(session).getTable(INDEX_STATISTICS_ENTRY_TABLE_NAME));

        // Remove existing statistics for the index
        removeStatistics(session, index);

        // Parent header row.
        Row row = new ValuesHolderRow (indexStatisticsRowType,
                        tableId, 
                        index.getIndexId(),
                        indexStatistics.getAnalysisTimestamp() / 1000,
                        indexStatistics.getRowCount(),
                        indexStatistics.getSampledCount());
        store.writeRow(session, row, null, null);
                       
         // Multi-column
        for(int prefixColumns = 1; prefixColumns <= index.getKeyColumns().size(); prefixColumns++) {
            Histogram histogram = indexStatistics.getHistogram(0, prefixColumns);
            if(histogram != null) {
                storeIndexStatisticsEntry(session,
                                          index,
                                          tableId,
                                          histogram.getColumnCount(),
                                          indexStatisticsEntryRowType,
                                          histogram);
            }
        }

        // Single-column
        for(int columnPosition = 1; columnPosition < index.getKeyColumns().size(); columnPosition++) {
            Histogram histogram = indexStatistics.getHistogram(columnPosition, 1);
            if(histogram != null) {
                storeIndexStatisticsEntry(session,
                                          index,
                                          tableId,
                                          -histogram.getFirstColumn() - 1,
                                          indexStatisticsEntryRowType,
                                          histogram);
            }
        }
    }

    private void storeIndexStatisticsEntry(Session session,
                                           Index index,
                                           int tableId,
                                           int columnCount,
                                           RowType indexStatisticsEntryRowType,
                                           Histogram histogram)
    {
        if (histogram != null) {
            int itemNumber = 0;
            for (HistogramEntry entry : histogram.getEntries()) {
                Row row = new ValuesHolderRow (indexStatisticsEntryRowType,
                            tableId,
                            index.getIndexId(),
                            columnCount,
                            ++itemNumber,
                            entry.getKeyString(),
                            entry.getKeyBytes(),
                            entry.getEqualCount(),
                            entry.getLessCount(),
                            entry.getDistinctCount());
                store.writeRow(session, row, null, null);
            }
        }
    }
}
