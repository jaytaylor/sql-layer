/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.store.MemoryStore;
import com.foundationdb.server.store.MemoryStoreData;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class MemoryStoreIndexStatistics extends AbstractStoreIndexStatistics<MemoryStore> implements VisitorCreator<Key,byte[]> {
    private static final Logger LOG = LoggerFactory.getLogger(MemoryStoreIndexStatistics.class);

    private final IndexStatisticsService indexStatisticsService;

    public MemoryStoreIndexStatistics(MemoryStore store, IndexStatisticsService indexStatisticsService) {
        super(store);
        this.indexStatisticsService = indexStatisticsService;
    }

    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        Table indexStatisticsTable = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        Schema schema = SchemaCache.globalSchema(getStore().getAIS(session));
        MemoryStoreData storeData = getStore().createStoreData(session, indexStatisticsTable.getGroup());
        storeData.persistitKey.append(indexStatisticsTable.getOrdinal())
                              .append((long)indexTable.getTableId())
                              .append((long)index.getIndexId());
        IndexStatistics result = null;
        getStore().groupKeyAndDescendantsIterator(session, storeData);
        while(storeData.next()) {
            if(result == null) {
                result = decodeHeader(storeData, index, schema);
            } else {
                decodeEntry(storeData, result, schema);
            }
        }
        if((result != null) && LOG.isDebugEnabled()) {
            LOG.debug("Loaded: {}", result.toString(index));
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        Table table = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        MemoryStoreData storeData = getStore().createStoreData(session, table.getGroup());
        storeData.persistitKey.clear();
        storeData.persistitKey.append(table.getOrdinal())
                              .append((long)indexTable.getTableId())
                              .append((long)index.getIndexId());
        getStore().groupKeyAndDescendantsIterator(session, storeData);
        while(storeData.next()) {
            Row row = getStore().expandRow(session, storeData, SchemaCache.globalSchema(index.getAIS()));
            getStore().deleteRow(session, row, false);
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        long indexRowCount = estimateIndexRowCount(session, index);
        IndexStatisticsVisitor<Key,byte[]> visitor = new IndexStatisticsVisitor<>(session,
                                                                                  index,
                                                                                  indexRowCount,
                                                                                  indexRowCount /*expectedCount*/,
                                                                                  this);
        int bucketCount = indexStatisticsService.bucketCount();
        visitor.init(bucketCount);
        MemoryStoreData storeData = getStore().createStoreData(session, index);
        // Whole index, forward.
        getStore().indexIterator(session, storeData, false);
        while(storeData.next()) {
            MemoryStore.unpackKey(storeData);
            visitor.visit(storeData.persistitKey, storeData.rawValue);
        }
        visitor.finish(bucketCount);
        IndexStatistics indexStatistics = visitor.getIndexStatistics();
        if(LOG.isDebugEnabled()) {
            LOG.debug("Analyzed: " + indexStatistics.toString(index));
        }
        return indexStatistics;
    }

    //
    // VisitorCreator
    //

    @Override
    public IndexStatisticsGenerator<Key,byte[]> multiColumnVisitor(Index index) {
        return new MemoryMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,byte[]> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new MemorySingleColumnIndexStatisticsVisitor(getStore(), indexColumn);
    }

    //
    // Internal
    //

    protected IndexStatistics decodeHeader(MemoryStoreData storeData, Index index, Schema schema) {
        return decodeIndexStatisticsRow(getStore().expandRow(storeData.session, storeData, schema), index);
    }

    protected void decodeEntry(MemoryStoreData storeData, IndexStatistics indexStatistics, Schema schema) {
        decodeIndexStatisticsEntryRow(getStore().expandRow(storeData.session, storeData, schema), indexStatistics);
    }
}
