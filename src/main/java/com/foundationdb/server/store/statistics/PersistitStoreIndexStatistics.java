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
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.server.api.dml.scan.LegacyRowWrapper;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class PersistitStoreIndexStatistics extends AbstractStoreIndexStatistics<PersistitStore> implements VisitorCreator<Key,Value> {
    private static final Logger logger = LoggerFactory.getLogger(PersistitStoreIndexStatistics.class);
    private static final int INITIAL_ROW_SIZE = 4096;

    private final IndexStatisticsService indexStatsService;


    public PersistitStoreIndexStatistics(PersistitStore store, IndexStatisticsService indexStatsService)
    {
        super(store);
        this.indexStatsService = indexStatsService;
    }


    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        try {
            return loadIndexStatisticsInternal(session, index);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        try {
            RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);
            Exchange exchange = getStore().getExchange(session, indexStatisticsRowDef);
            removeStatisticsInternal(session, index, exchange);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        long estimatedRowCount = estimateIndexRowCount(session, index);
        IndexStatisticsVisitor<Key,Value> visitor = new IndexStatisticsVisitor<>(session, index, estimatedRowCount, estimatedRowCount, this);
        int bucketCount = indexStatsService.bucketCount();
        visitor.init(bucketCount);
        getStore().traverse(session, index, visitor, scanTimeLimit, sleepTime);
        visitor.finish(bucketCount);
        IndexStatistics indexStatistics = visitor.getIndexStatistics();
        if (logger.isDebugEnabled()) {
            logger.debug("Analyzed: " + indexStatistics.toString(index));
        }
        return indexStatistics;
    }


    //
    // VisitorCreator
    //

    @Override
    public IndexStatisticsGenerator<Key,Value> multiColumnVisitor(Index index) {
        return new PersistitMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,Value> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new PersistitSingleColumnIndexStatisticsVisitor(getStore(), session, indexColumn);
    }


    //
    // Internal
    //

    private IndexStatistics loadIndexStatisticsInternal(Session session, Index index) throws PersistitException {
        RowDef indexRowDef = index.leafMostTable().rowDef();
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);
        RowDef indexStatisticsEntryRowDef = getIndexStatsEntryRowDef(session);

        Exchange exchange = getStore().getExchange(session, indexStatisticsRowDef);
        exchange.clear()
            .append(indexStatisticsRowDef.table().getOrdinal())
            .append((long)indexRowDef.getRowDefId())
            .append((long)index.getIndexId());
        if (!exchange.fetch().getValue().isDefined()) {
            return null;
        }
        IndexStatistics result = decodeHeader(session, exchange, indexStatisticsRowDef, index);
        while (exchange.traverse(Key.GT, true)) {
            if (exchange.getKey().getDepth() <= indexStatisticsRowDef.getHKeyDepth()) {
                break;          // End of children.
            }
            decodeEntry(session, exchange, indexStatisticsEntryRowDef, result);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded: " + result.toString(index));
        }
        return result;
    }

    protected IndexStatistics decodeHeader(Session session,
                                           Exchange exchange,
                                           RowDef indexStatisticsRowDef,
                                           Index index) {
        RowData rowData = new RowData(new byte[exchange.getValue().getEncodedSize() + RowData.ENVELOPE_SIZE]);
        getStore().expandRowData(session, exchange, rowData);
        return decodeIndexStatisticsRow(rowData, indexStatisticsRowDef, index);
    }

    protected void decodeEntry(Session session,
                               Exchange exchange,
                               RowDef indexStatisticsEntryRowDef,
                               IndexStatistics indexStatistics) {
        RowData rowData = new RowData(new byte[exchange.getValue().getEncodedSize() + RowData.ENVELOPE_SIZE]);
        getStore().expandRowData(session, exchange, rowData);
        decodeIndexStatisticsEntryRow(rowData, indexStatisticsEntryRowDef, indexStatistics);
    }

    private void removeStatisticsInternal(Session session, Index index, Exchange exchange) throws PersistitException {
        RowData rowData = new RowData(new byte[INITIAL_ROW_SIZE]);
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);
        RowDef indexStatisticsEntryRowDef = getIndexStatsEntryRowDef(session);
        int tableId = index.leafMostTable().rowDef().getRowDefId();
        int indexId = index.getIndexId();
        // Delete index_statistics_entry rows.
        exchange.append(Key.BEFORE);
        while (exchange.traverse(Key.Direction.GT, true)) {
            getStore().expandRowData(session, exchange, rowData);
            if (rowData.getRowDefId() == indexStatisticsEntryRowDef.getRowDefId() &&
                selectedIndex(indexStatisticsEntryRowDef, rowData, tableId, indexId)) {
                getStore().deleteRow(session, rowData, false);
            }
        }

        // Delete only the parent index_statistics row
        exchange.clear().append(Key.BEFORE);
        while (exchange.traverse(Key.Direction.GT, true)) {
            getStore().expandRowData(session, exchange, rowData);
            if (rowData.getRowDefId() == indexStatisticsRowDef.getRowDefId() &&
                selectedIndex(indexStatisticsRowDef, rowData, tableId, indexId)) {
                getStore().deleteRow(session, rowData, false);
            }
        }
    }

    private boolean selectedIndex(RowDef rowDef, RowData rowData, long tableId, long indexId)
    {
        LegacyRowWrapper row = new LegacyRowWrapper(rowDef, rowData);
        long rowTableId = (Long) row.get(0);
        long rowIndexId = (Long) row.get(1);
        return rowTableId == tableId && rowIndexId == indexId;
    }
}
