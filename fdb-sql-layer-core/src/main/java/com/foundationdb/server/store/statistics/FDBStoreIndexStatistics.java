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
import com.foundationdb.server.error.QueryCanceledException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.rowdata.RowDef;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBStore;
import com.foundationdb.server.store.FDBStoreData;
import com.foundationdb.server.store.FDBStoreDataHelper;
import com.foundationdb.server.store.FDBTransactionService;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        RowDef indexRowDef = index.leafMostTable().rowDef();
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);
        RowDef indexStatisticsEntryRowDef = getIndexStatsEntryRowDef(session);

        Key hKey = getStore().createKey();
        hKey.append(indexStatisticsRowDef.table().getOrdinal())
            .append((long) indexRowDef.getRowDefId())
            .append((long) index.getIndexId());

        IndexStatistics result = null;
        FDBStoreData storeData = getStore().createStoreData(session, indexStatisticsRowDef.getGroup());
        hKey.copyTo(storeData.persistitKey);
        getStore().groupKeyAndDescendantsIterator(session, storeData, true);
        while(storeData.next()) {
            if(result == null) {
                result = decodeHeader(storeData, indexStatisticsRowDef, index);
            } else {
                decodeEntry(storeData, indexStatisticsEntryRowDef, result);
            }
        }
        if ((result != null) && logger.isDebugEnabled()) {
            logger.debug("Loaded: " + result.toString(index));
        }
        return result;
    }

    @Override
    public void removeStatistics(Session session, Index index) {
        RowDef indexRowDef = index.leafMostTable().rowDef();
        RowDef indexStatisticsRowDef = getIndexStatsRowDef(session);

        Key hKey = getStore().createKey();
        hKey.append(indexStatisticsRowDef.table().getOrdinal())
                .append((long) indexRowDef.getRowDefId())
                .append((long) index.getIndexId());

        FDBStoreData storeData = getStore().createStoreData(session, indexStatisticsRowDef.getGroup());
        hKey.copyTo(storeData.persistitKey);
        getStore().groupKeyAndDescendantsIterator(session, storeData, false);
        while(storeData.next()) {
            RowData rowData = new RowData();
            FDBStoreDataHelper.expandRowData(rowData, storeData, false);
            getStore().deleteRow(session, rowData, false); // TODO: Use cascade?
        }
    }

    @Override
    public IndexStatistics computeIndexStatistics(Session session, Index index, long scanTimeLimit, long sleepTime) {
        FDBTransactionService.TransactionState txn = null;
        long nextCommitTime = 0;
        if (scanTimeLimit >= 0) {
            txn = txnService.getTransaction(session);
            nextCommitTime = txn.getStartTime() + scanTimeLimit;
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
        // Whole index, snapshot
        getStore().indexIterator(session, storeData, false, false, false, true);
        while(storeData.next()) {
            if (++skippedSamples < sampleRate)
                continue;       // This value not sampled.
            skippedSamples = 0;
            FDBStoreDataHelper.unpackKey(storeData);
            // TODO: Does anything look at rawValue?
            visitor.visit(storeData.persistitKey, storeData.rawValue);
            if ((scanTimeLimit >= 0) &&
                (System.currentTimeMillis() >= nextCommitTime)) {
                storeData.closeIterator();
                txn.commitAndReset(session);
                if (sleepTime > 0) {
                    try {
                        Thread.sleep(sleepTime);
                    }
                    catch (InterruptedException ex) {
                        throw new QueryCanceledException(session);
                    }
                }
                nextCommitTime = txn.getStartTime() + scanTimeLimit;
                // Start at key, non-inclusive, snapshot
                getStore().indexIterator(session, storeData, true, false, false, true);
            }
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
                                           RowDef indexStatisticsRowDef,
                                           Index index) {
        RowData rowData = new RowData();
        FDBStoreDataHelper.expandRowData(rowData, storeData, false);
        return decodeIndexStatisticsRow(rowData, indexStatisticsRowDef, index);
    }

    protected void decodeEntry(FDBStoreData storeData,
                               RowDef indexStatisticsEntryRowDef,
                               IndexStatistics indexStatistics) {
        RowData rowData = new RowData();
        FDBStoreDataHelper.expandRowData(rowData, storeData, false);
        decodeIndexStatisticsEntryRow(rowData, indexStatisticsEntryRowDef, indexStatistics);
    }
}
