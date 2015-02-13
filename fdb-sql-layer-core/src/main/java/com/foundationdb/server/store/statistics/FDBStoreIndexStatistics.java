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
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBScanTransactionOptions;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.foundationdb.server.store.FDBTransactionService;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.foundationdb.server.store.statistics.IndexStatisticsService.INDEX_STATISTICS_TABLE_NAME;
import static com.foundationdb.server.store.statistics.IndexStatisticsVisitor.VisitorCreator;

public class FDBStoreIndexStatistics extends AbstractStoreIndexStatistics<FDBStore> implements VisitorCreator<Key,byte[]> {
    public static final String SAMPLER_COUNT_LIMIT_PROPERTY = "fdbsql.index_statistics.sampler_count_limit";
    private static final Logger logger = LoggerFactory.getLogger(FDBStoreIndexStatistics.class);

    private final IndexStatisticsService indexStatisticsService;
    private final FDBTransactionService txnService;
    private final long samplerCountLimit;

    public FDBStoreIndexStatistics(FDBStore store, IndexStatisticsService indexStatisticsService, TransactionService txnService, ConfigurationService configurationService) {
        super(store);
        this.indexStatisticsService = indexStatisticsService;
        this.txnService = (FDBTransactionService)txnService;
        this.samplerCountLimit = Long.parseLong(configurationService.getProperty(SAMPLER_COUNT_LIMIT_PROPERTY));
    }


    //
    // AbstractStoreIndexStatistics
    //

    @Override
    public IndexStatistics loadIndexStatistics(Session session, Index index) {
        Table indexStatisticsTable = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        Schema schema = SchemaCache.globalSchema(getStore().getAIS(session));
        FDBStoreData storeData = getStore().createStoreData(session, indexStatisticsTable.getGroup());
        storeData.persistitKey.append(indexStatisticsTable.getOrdinal())
        .append((long) indexTable.getTableId())
        .append((long) index.getIndexId());

        IndexStatistics result = null;
        getStore().groupKeyAndDescendantsIterator(session, storeData, FDBScanTransactionOptions.SNAPSHOT);
        while(storeData.next()) {
            if(result == null) {
                result = decodeHeader(storeData, index, schema);
            } else {
                decodeEntry(storeData, result, schema);
            }
        }
        if ((result != null) && logger.isDebugEnabled()) {
            logger.debug("Loaded: " + result.toString(index));
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        Table table = getStore().getAIS(session).getTable(INDEX_STATISTICS_TABLE_NAME);
        Table indexTable = index.leafMostTable();
        FDBStoreData storeData = getStore().createStoreData(session, table.getGroup());
        storeData.persistitKey.clear();

        storeData.persistitKey.append(table.getOrdinal())
            .append((long) indexTable.getTableId())
            .append((long) index.getIndexId());
        getStore().groupKeyAndDescendantsIterator(session, storeData, FDBScanTransactionOptions.NORMAL);
        while(storeData.next()) {
            Row row = FDBStoreDataHelper.expandRow(SchemaCache.globalSchema(index.getAIS()), storeData);
            getStore().deleteRow(session, row, false);
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        FDBScanTransactionOptions transactionOptions;
        if (scanTimeLimit > 0) {
            transactionOptions = new FDBScanTransactionOptions(true, -1,
                                                               scanTimeLimit, sleepTime);
        }
        else {
            transactionOptions = FDBScanTransactionOptions.SNAPSHOT;
        }
        long indexRowCount = estimateIndexRowCount(session, index);
        long expectedSampleCount = indexRowCount;
        int sampleRate = 1, skippedSamples = 0;
        int nSingle = index.getKeyColumns().size() - 1;
        if (nSingle > 0) {
            // Multi-column index might need sampling.  In the worst case, the visitor
            // will need to retain one copy of the key for each non-leading column for
            // each sampled row. Keep that below samplerCountLimit by sampling every few
            // rows. We could still send everything for the leading column, except that
            // the sample count applies to the whole, not per histograms.
            sampleRate = (int)((indexRowCount * nSingle + samplerCountLimit - 1) / samplerCountLimit); // Round up.
            if (sampleRate > 1) {
                expectedSampleCount = indexRowCount / sampleRate;
                logger.debug("Sampling rate for {} is {}", index, sampleRate);
            }
        }
        IndexStatisticsVisitor<Key,byte[]> visitor = new IndexStatisticsVisitor<>(session, index, indexRowCount, expectedSampleCount, this);
        int bucketCount = indexStatisticsService.bucketCount();
        visitor.init(bucketCount);
        FDBStoreData storeData = getStore().createStoreData(session, index);
        // Whole index, forward.
        getStore().indexIterator(session, storeData, false, false, true, false,
                                 transactionOptions);
        while(storeData.next()) {
            if (++skippedSamples < sampleRate)
                continue;       // This value not sampled.
            skippedSamples = 0;
            FDBStoreDataHelper.unpackKey(storeData);
            // TODO: Does anything look at rawValue?
            visitor.visit(storeData.persistitKey, storeData.rawValue);
        }
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
    public IndexStatisticsGenerator<Key,byte[]> multiColumnVisitor(Index index) {
        return new FDBMultiColumnIndexStatisticsVisitor(index, getStore());
    }

    @Override
    public IndexStatisticsGenerator<Key,byte[]> singleColumnVisitor(Session session, IndexColumn indexColumn) {
        return new FDBSingleColumnIndexStatisticsVisitor(getStore(), indexColumn);
    }


    //
    // Internal
    //

    protected IndexStatistics decodeHeader(FDBStoreData storeData,
                                           Index index, 
                                           Schema schema) {
        return decodeIndexStatisticsRow(FDBStoreDataHelper.expandRow(schema, storeData), index);
    }

    protected void decodeEntry(FDBStoreData storeData,
                               IndexStatistics indexStatistics,
                               Schema schema) {
        decodeIndexStatisticsEntryRow(FDBStoreDataHelper.expandRow(schema, storeData), indexStatistics);
    }
}
