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

package com.akiban.server.store.statistics;

import static com.akiban.server.store.statistics.IndexStatistics.*;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableName;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;

import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Manage index statistics for / stored in Persistit
 */
public class PersistitStoreIndexStatistics
{
    private static final Logger logger = LoggerFactory.getLogger(PersistitStoreIndexStatistics.class);

    private final PersistitStore store;
    private final TreeService treeService;

    /** Initialize index statistics manager for the given store. */
    public PersistitStoreIndexStatistics(PersistitStore store, TreeService treeService) {
        this.store = store;
        this.treeService = treeService;
    }

    /** Load previously stored statistics from database. */
    public IndexStatistics loadIndexStatistics(Session session, Index index)
            throws PersistitException {
        IndexDef indexDef = (IndexDef)index.indexDef();
        RowDef indexStatisticsRowDef = store.getRowDefCache()
            .getRowDef(INDEX_STATISTICS_TABLE_NAME);
        RowDef indexStatisticsEntryRowDef = store.getRowDefCache()
            .getRowDef(INDEX_STATISTICS_ENTRY_TABLE_NAME);

        Exchange exchange = store.getExchange(session, indexStatisticsRowDef);
        exchange.clear()
            .append(indexStatisticsRowDef.getOrdinal())
            .append((long)indexDef.getRowDef().getRowDefId())
            .append((long)index.getIndexId());
        if (!exchange.fetch().getValue().isDefined())
            return null;
        IndexStatistics result = decodeHeader(exchange, indexStatisticsRowDef, 
                                              index);
        while (exchange.traverse(Key.GT, true)) {
            if (exchange.getKey().getDepth() <= indexStatisticsRowDef.getHKeyDepth())
                break;          // End of children.
            decodeEntry(exchange, indexStatisticsEntryRowDef, result);
        }
        logger.debug("Loaded: " + result);
        return result;
    }

    /* Storage formats.
     * Keep in sync with akiban_information_schema.
     */

    private static final TableName INDEX_STATISTICS_TABLE_NAME = new TableName("akiban_information_schema", "zindex_statistics");
    private static final int TABLE_ID_FIELD_INDEX = 0;
    private static final int INDEX_ID_FIELD_INDEX = 1;
    private static final int ANALYSIS_TIMESTAMP_FIELD_INDEX = 2;
    private static final int ROW_COUNT_FIELD_INDEX = 3;
    private static final int SAMPLED_COUNT_FIELD_INDEX = 4;

    private static final TableName INDEX_STATISTICS_ENTRY_TABLE_NAME = new TableName("akiban_information_schema", "zindex_statistics_entry");
    // Parent keys the same.
    private static final int COLUMN_COUNT_FIELD_INDEX = 2;
    private static final int ITEM_NUMBER_FIELD_INDEX = 3;
    private static final int KEY_STRING_FIELD_INDEX = 4;
    private static final int KEY_BYTES_FIELD_INDEX = 5;
    private static final int EQ_COUNT_FIELD_INDEX = 6;
    private static final int LT_COUNT_FIELD_INDEX = 7;
    private static final int DISTINCT_COUNT_FIELD_INDEX = 8;

    private static final int MAX_TRANSACTION_RETRY_COUNT = 10;
    private static final boolean forceToDisk = false;
    private static final int INITIAL_ROW_SIZE = 4096;

    protected IndexStatistics decodeHeader(Exchange exchange, RowDef indexStatisticsRowDef,
                                           Index index)
            throws PersistitException {
        RowData rowData = new RowData(new byte[exchange.getValue().getEncodedSize() + RowData.ENVELOPE_SIZE]);
        store.expandRowData(exchange, rowData);
        long analysisTimestampLocation =
            indexStatisticsRowDef.fieldLocation(rowData, ANALYSIS_TIMESTAMP_FIELD_INDEX);
        long analysisTimestamp = rowData.getIntegerValue((int)analysisTimestampLocation,
                                                         (int)(analysisTimestampLocation >>> 32));
        long rowCountLocation =
            indexStatisticsRowDef.fieldLocation(rowData, ROW_COUNT_FIELD_INDEX);
        long rowCount = rowData.getIntegerValue((int)rowCountLocation,
                                                (int)(rowCountLocation >>> 32));
        long sampledCountLocation =
            indexStatisticsRowDef.fieldLocation(rowData, SAMPLED_COUNT_FIELD_INDEX);
        long sampledCount = rowData.getIntegerValue((int)sampledCountLocation,
                                                    (int)(sampledCountLocation >>> 32));
        IndexStatistics result = new IndexStatistics(index);
        result.setAnalysisTimestamp(analysisTimestamp);
        result.setRowCount(rowCount);
        result.setSampledCount(sampledCount);
        return result;
    }

    protected void decodeEntry(Exchange exchange, RowDef indexStatisticsEntryRowDef,
                               IndexStatistics indexStatistics)
            throws PersistitException {
        RowData rowData = new RowData(new byte[exchange.getValue().getEncodedSize() + RowData.ENVELOPE_SIZE]);
        store.expandRowData(exchange, rowData);
        long columnCountLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, COLUMN_COUNT_FIELD_INDEX);
        int columnCount = (int)rowData.getIntegerValue((int)columnCountLocation,
                                                       (int)(columnCountLocation >>> 32));
        long itemNumberLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, ITEM_NUMBER_FIELD_INDEX);
        int itemNumber = (int)rowData.getIntegerValue((int)itemNumberLocation,
                                                      (int)(itemNumberLocation >>> 32));
        long keyStringLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, KEY_STRING_FIELD_INDEX);
        String keyString = rowData.getStringValue((int)keyStringLocation,
                                                  (int)(keyStringLocation >>> 32),
                                                  indexStatisticsEntryRowDef.getFieldDef(KEY_STRING_FIELD_INDEX));
        long keyBytesLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, KEY_BYTES_FIELD_INDEX);
        final int keyBytesPrefix = indexStatisticsEntryRowDef.getFieldDef(KEY_BYTES_FIELD_INDEX).getPrefixSize();
        byte[] keyBytes = new byte[(int)(keyBytesLocation >>> 32) - keyBytesPrefix];
        System.arraycopy(rowData.getBytes(), (int)keyBytesLocation + keyBytesPrefix,
                         keyBytes, 0, keyBytes.length);
        long eqCountLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, EQ_COUNT_FIELD_INDEX);
        long eqCount = rowData.getIntegerValue((int)eqCountLocation,
                                               (int)(eqCountLocation >>> 32));
        long ltCountLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, LT_COUNT_FIELD_INDEX);
        long ltCount = rowData.getIntegerValue((int)ltCountLocation,
                                               (int)(ltCountLocation >>> 32));
        long distinctCountLocation =
            indexStatisticsEntryRowDef.fieldLocation(rowData, DISTINCT_COUNT_FIELD_INDEX);
        long distinctCount = rowData.getIntegerValue((int)distinctCountLocation,
                                                     (int)(distinctCountLocation >>> 32));
        Histogram histogram = indexStatistics.getHistogram(columnCount);
        if (histogram == null) {
            histogram = new Histogram(indexStatistics.getIndex(), columnCount,
                                      new ArrayList<HistogramEntry>());
            indexStatistics.addHistogram(histogram);
        }
        histogram.getEntries().add(new HistogramEntry(keyString, keyBytes,
                                                      eqCount, ltCount, distinctCount));
    }

    /** Store statistics into database. */
    public void storeIndexStatistics(Session session, IndexStatistics indexStatistics)
            throws PersistitException {
        Index index = indexStatistics.getIndex();
        IndexDef indexDef = (IndexDef)index.indexDef();
        RowDef indexStatisticsRowDef = store.getRowDefCache()
            .getRowDef(INDEX_STATISTICS_TABLE_NAME);
        RowDef indexStatisticsEntryRowDef = store.getRowDefCache()
            .getRowDef(INDEX_STATISTICS_ENTRY_TABLE_NAME);
        Exchange exchange = store.getExchange(session, indexStatisticsRowDef);
        Transaction transaction = exchange.getTransaction();
        int retries = MAX_TRANSACTION_RETRY_COUNT;

        for (;;) {
            transaction.begin();
            try {
                RowData rowData = new RowData(new byte[INITIAL_ROW_SIZE]);

                // First delete any old entries.
                rowData.createRow(indexStatisticsRowDef, new Object[] {
                                      indexDef.getRowDef().getRowDefId(),
                                      index.getIndexId() 
                                  });
                store.constructHKey(session, exchange, 
                                    indexStatisticsRowDef, rowData, false);
                exchange.cut();
                exchange.remove(Key.GT);
                // TODO: Need to get a count back from
                // exchange.remove() in order to tell the row count in
                // treeService.getTableStatusCache() what changed.

                // Parent header row.
                rowData.createRow(indexStatisticsRowDef, new Object[] {
                                      indexDef.getRowDef().getRowDefId(),
                                      index.getIndexId(),
                                      indexStatistics.getAnalysisTimestamp(),
                                      indexStatistics.getRowCount(),
                                      indexStatistics.getSampledCount()
                                  });
                try {
                    store.writeRow(session, rowData);
                } 
                catch (InvalidOperationException ex) {
                    throw new RollbackException(ex);
                }
                
                for (int i = 0; i < index.getColumns().size(); i++) {
                    Histogram histogram = indexStatistics.getHistogram(i + 1);
                    int itemNumber = 0;
                    for (HistogramEntry entry : histogram.getEntries()) {
                        rowData.createRow(indexStatisticsEntryRowDef, new Object[] {
                                              indexDef.getRowDef().getRowDefId(),
                                              index.getIndexId(),
                                              histogram.getColumnCount(),
                                              ++itemNumber,
                                              entry.getKeyString(),
                                              entry.getKeyBytes(),
                                              entry.getEqualCount(),
                                              entry.getLessCount(),
                                              entry.getDistinctCount()
                                          });
                        try {
                            store.writeRow(session, rowData);
                        } 
                        catch (InvalidOperationException ex) {
                            throw new RollbackException(ex);
                        }
                    }
                }
                transaction.commit(forceToDisk);
                break;
            }
            catch (RollbackException ex) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } 
            finally {
                transaction.end();
            }
        }
    }

    /** Delete any stored statistics for the given index. */
    public void deleteIndexStatistics(Session session, Index index)
            throws PersistitException {
        IndexDef indexDef = (IndexDef)index.indexDef();
        RowDef indexStatisticsRowDef = store.getRowDefCache()
            .getRowDef(INDEX_STATISTICS_TABLE_NAME);
        Exchange exchange = store.getExchange(session, indexStatisticsRowDef);
        Transaction transaction = exchange.getTransaction();
        int retries = MAX_TRANSACTION_RETRY_COUNT;

        for (;;) {
            transaction.begin();
            try {
                RowData rowData = new RowData(new byte[INITIAL_ROW_SIZE]);
                rowData.createRow(indexStatisticsRowDef, new Object[] {
                                      indexDef.getRowDef().getRowDefId(),
                                      index.getIndexId() 
                                  });
                store.constructHKey(session, exchange, 
                                    indexStatisticsRowDef, rowData, false);
                exchange.cut();
                exchange.remove(Key.GT);
                // TODO: See exchange.remove() above.
                transaction.commit(forceToDisk);
                break;
            } 
            catch (RollbackException ex) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } 
            finally {
                transaction.end();
            }
        }
    }
    
    /** Sample index values and build statistics histograms. */
    public IndexStatistics computeIndexStatistics(Session session, Index index)
            throws PersistitException {
        PersistitIndexStatisticsVisitor visitor = 
            new PersistitIndexStatisticsVisitor(index);
        store.traverse(session, index, visitor);
        IndexStatistics result = visitor.getIndexStatistics();
        logger.debug("Analyzed: " + result);
        return result;
    }
    
    // TODO: Is this the right API?
    public void analyzeIndexes(Session session, Collection<? extends Index> indexes) {
        for (Index index : indexes) {
            try {
                IndexStatistics indexStatistics = computeIndexStatistics(session, index);
                if (indexStatistics != null) {
                    logger.debug("Analyzed: " + indexStatistics);
                    storeIndexStatistics(session, indexStatistics);
                }
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
        }
    }

}
