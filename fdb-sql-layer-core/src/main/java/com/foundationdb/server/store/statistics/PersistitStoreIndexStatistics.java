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
import com.foundationdb.ais.model.Table;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.PersistitStore;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Value;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class PersistitStoreIndexStatistics extends AbstractStoreIndexStatistics<PersistitStore> implements VisitorCreator<Key,Value> {
    private static final Logger logger = LoggerFactory.getLogger(PersistitStoreIndexStatistics.class);

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
            RowType indexStatisticsRowType = getIndexStatsRowType(session);
            Exchange exchange = getStore().getExchange(session, indexStatisticsRowType.table().getGroup());
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
        Schema schema = SchemaCache.globalSchema(index.getAIS());
        RowType indexRowType = schema.tableRowType(index.leafMostTable());
        Table indexStatisticsTable = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        
        Exchange exchange = getStore().getExchange(session, indexStatisticsTable.getGroup());
        exchange.clear()
            .append(indexStatisticsTable.getOrdinal())
            .append((long)indexRowType.table().getTableId())
            .append((long)index.getIndexId());
        if (!exchange.fetch().getValue().isDefined()) {
            return null;
        }
        IndexStatistics result = decodeHeader (session, exchange, index, schema);
        int depth = exchange.getKey().getDepth();
        while (exchange.traverse(Key.GT, true)) {
            if (exchange.getKey().getDepth() <= depth) {
                break;          // End of children.
            }
            decodeEntry(session, exchange, result, schema);
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded: " + result.toString(index));
        }
        return result;
    }

    protected IndexStatistics decodeHeader(Session session,
                                           Exchange exchange,
                                           Index index, 
                                           Schema schema) {
        Row row = getStore().expandRow(session, exchange, schema);
        return decodeIndexStatisticsRow(row, index);
    }

    protected void decodeEntry (Session session, Exchange exchange, IndexStatistics indexStatistics, Schema schema) {
        Row row = getStore().expandRow(session, exchange, schema);
        decodeIndexStatisticsEntryRow(row, indexStatistics);
        
    }
    
    private void removeStatisticsInternal(Session session, Index index, Exchange exchange) throws PersistitException {
        int tableId = index.leafMostTable().getTableId();
        int indexId = index.getIndexId();
        Schema schema = SchemaCache.globalSchema(index.getAIS());
        RowType indexStatisticsRowType = getIndexStatsRowType(session);
        RowType indexStatisticsEntryRowType = getIndexStatsEntryRowType(session);
        exchange.append(Key.BEFORE);
        while (exchange.traverse(Key.Direction.GT, true)) {
            Row row = getStore().expandRow(session, exchange, schema);
            if (row.rowType() == indexStatisticsEntryRowType &&
                    selectedIndex(row, tableId, indexId)) {
                getStore().deleteRow(session, row, false);
                
            }
        }
        
        exchange.clear().append(Key.BEFORE);
        while (exchange.traverse(Key.Direction.GT, true)) {
            Row row = getStore().expandRow(session, exchange, schema);
            if (row.rowType() == indexStatisticsRowType && 
                    selectedIndex(row, tableId, indexId)) {
                getStore().deleteRow(session, row, false);
            }
        }
    }

    private boolean selectedIndex(Row row, long tableId, long indexId) {
        long rowTableId = row.value(0).getInt64();
        long rowIndexId = row.value(1).getInt64();
        return rowTableId == tableId && rowIndexId == indexId;
    }
}
