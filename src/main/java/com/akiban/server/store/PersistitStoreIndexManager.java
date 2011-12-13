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

package com.akiban.server.store;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexToHKey;
import com.akiban.ais.model.TableName;
import com.akiban.server.rowdata.IndexDef;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.rowdata.RowDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.akiban.server.TableStatistics;
import com.akiban.server.TableStatistics.Histogram;
import com.akiban.server.TableStatistics.HistogramSample;
import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.error.PersistItErrorException;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.tree.TreeService;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.KeyHistogram;
import com.persistit.KeyHistogram.KeyCount;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;
import com.persistit.exception.TransactionFailedException;

public class PersistitStoreIndexManager implements IndexManager {

    private static final Logger LOG = LoggerFactory
            .getLogger(PersistitStoreIndexManager.class.getName());

    private final static TableName ANALYSIS_TABLE_NAME = new TableName("akiban_information_schema", "index_analysis");

    private final static int DEFAULT_SAMPLE_SIZE = 32;

    private final static int STARTING_TREE_DEPTH = 2;

    /**
     * Field number in index_analysis table. Change if akiban_inormation_schema
     * changes.
     */
    private final static int ROW_COUNT_FIELD_INDEX = 6;

    /**
     * Field number in index_analysis table. Change if akiban_inormation_schema
     * changes.
     */
    private final static int ROW_DATA_FIELD_INDEX = 5;

    private final static int ROW_DATA_LENGTH = 4096;

    private final static int INDEX_LEVEL_MULTIPLIER = 200;

    private final static int SAMPLE_SIZE_MULTIPLIER = 32;

    private final PersistitStore store;

    private final TreeService treeService;

    public PersistitStoreIndexManager(final PersistitStore store,
            final TreeService treeService) {

        this.store = store;
        this.treeService = treeService;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.akiban.server.store.IndexManager#analyzeTable(com.akiban.server.service
     * .session.Session, com.akiban.server.rowdata.RowDef)
     */
    @Override
    public void analyzeTable(final Session session, final RowDef rowDef) {
        analyzeTable(session, rowDef, DEFAULT_SAMPLE_SIZE - 1);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.akiban.server.store.IndexManager#analyzeTable(com.akiban.server.service
     * .session.Session, com.akiban.server.rowdata.RowDef, int)
     */
    @Override
    public void analyzeTable(final Session session, final RowDef rowDef, final int sampleSize) {
        for (Index index : rowDef.getIndexes()) {
            try {
                analyzeIndex(session, index, sampleSize);
            } catch (PersistitException e) {
                throw new PersistItErrorException (e);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.akiban.server.store.IndexManager#deleteIndexAnalysis(com.akiban.server
     * .service.session.Session, com.akiban.server.IndexDef)
     */
    @Override
    public void deleteIndexAnalysis(final Session session, final Index index) throws PersistitException {
        final RowDef indexAnalysisRowDef = store.getRowDefCache().getRowDef(
                ANALYSIS_TABLE_NAME);
        if (indexAnalysisRowDef == null) {
            // true for some unit tests
            return;
        }
        final Exchange analysisEx = store.getExchange(session, indexAnalysisRowDef);
        final Transaction transaction = analysisEx.getTransaction();

        int retries = PersistitStore.MAX_TRANSACTION_RETRY_COUNT;

        for (;;) {
            transaction.begin();
            try {
                RowData rowData = new RowData(new byte[ROW_DATA_LENGTH]);
                IndexDef indexDef = (IndexDef)index.indexDef();
                rowData.createRow(indexAnalysisRowDef, new Object[] {
                        indexDef.getRowDef().getRowDefId(), index.getIndexId() });
                //
                // Remove previous analysis
                //
                try {
                    store.constructHKey(session, analysisEx, indexAnalysisRowDef, rowData, false);
                    analysisEx.getKey().cut();
                    analysisEx.remove(Key.GT);
                } catch (PersistitException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                treeService.getTableStatusCache().truncate(indexAnalysisRowDef.getRowDefId());
                transaction.commit(store.forceToDisk);
                break;

            } catch (RollbackException re) {
                if (--retries < 0) {
                    throw new TransactionFailedException();
                }
            } finally {
                transaction.end();
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.akiban.server.store.IndexManager#analyzeIndex(com.akiban.server.service
     * .session.Session, com.akiban.server.IndexDef, int)
     */
    @Override
    public void analyzeIndex(final Session session, final Index index, final int sampleSize) throws PersistitException {
        final Exchange probeEx;
        final Key startKey;
        final Key endKey;
        final int keyDepth;
        KeyFilter keyFilter = null;

        IndexDef indexDef = (IndexDef)index.indexDef();
        if (index.isHKeyEquivalent()) {
            probeEx = store.getExchange(session, indexDef.getRowDef());
            startKey = new Key(store.getDb());
            endKey = new Key(store.getDb());
            final IndexToHKey indexToHKey = index.indexToHKey();
            assert indexToHKey.isOrdinal(0) : indexToHKey;
            final KeyFilter.Term[] terms = new KeyFilter.Term[indexToHKey.getLength()];
            startKey.append(indexToHKey.getOrdinal(0));
            endKey.append(indexToHKey.getOrdinal(0));
            terms[0] = KeyFilter.simpleTerm(indexToHKey.getOrdinal(0));
            startKey.append(Key.BEFORE);
            endKey.append(Key.AFTER);
            keyDepth = indexToHKey.getLength();
            for (int depth = 1; depth < indexToHKey.getLength(); depth++) {
                terms[depth] = KeyFilter.ALL;
            }
            keyFilter = new KeyFilter(terms, terms.length, Integer.MAX_VALUE);

        } else {
            probeEx = store.getExchange(session, index);
            startKey = Key.LEFT_GUARD_KEY;
            endKey = Key.RIGHT_GUARD_KEY;
            keyDepth = indexDef.getFields().length;
        }

        // First try to enumerate the values. If there are more than
        KeyHistogram keyHistogram = null;
        int treeLevel = Math.max(0, probeEx.getTree().getDepth()
                - STARTING_TREE_DEPTH);
        while (treeLevel >= 0) {
            if (treeLevel == 0 && keyFilter != null) {
                // At leaf level of an htable - here we limit the
                // keyFilter depth to count only keys that match exactly.
                keyFilter = keyFilter.limit(keyFilter.getMinimumDepth(),
                        keyFilter.getMinimumDepth());
            }
            keyHistogram = probeEx.computeHistogram(startKey, endKey,
                    sampleSize, keyDepth, keyFilter, treeLevel);
            if (keyHistogram.getKeyCount() > sampleSize
                    * SAMPLE_SIZE_MULTIPLIER) {
                break;
            }
            treeLevel = keyHistogram.getTreeDepth() - 1;
        }

        if (LOG.isInfoEnabled()) {
            LOG.info(String.format("Analyzed index %s in table %s: %,d keys "
                    + "at keyDepth/treeLevel %d/%d", index.getIndexName().getName(),
                    indexDef.getRowDef().getTableName(),
                    keyHistogram.getKeyCount(), keyDepth,
                    Math.max(0, treeLevel)));
        }

        final RowDef indexAnalysisRowDef = store.getRowDefCache().getRowDef(
                ANALYSIS_TABLE_NAME);
        final Exchange analysisEx = store.getExchange(session, indexAnalysisRowDef);
        final Transaction transaction = analysisEx.getTransaction();
        final long now = System.currentTimeMillis() / 1000;
        final KeyHistogram keyHistogram0 = keyHistogram;
        final int multiplier = (int) (Math.pow(INDEX_LEVEL_MULTIPLIER,
                keyHistogram.getTreeDepth()));
        final Key key = new Key((Persistit) null);
        final RowData rowData = new RowData(new byte[ROW_DATA_LENGTH]);
        final RowData indexRowData = new RowData(new byte[ROW_DATA_LENGTH]);
        final Object[] indexValues = new Object[indexDef.getRowDef().getFieldCount()];

        try {

            int retries = PersistitStore.MAX_TRANSACTION_RETRY_COUNT;
            for (;;) {
                transaction.begin();
                try {

                    rowData.createRow(indexAnalysisRowDef,
                            new Object[] { indexDef.getRowDef().getRowDefId(),
                                           index.getIndexId(), now, 0, "", null, 0 });
                    //
                    // Remove previous analysis
                    //
                    try {
                        store.constructHKey(session, analysisEx,
                                indexAnalysisRowDef, rowData, false);
                        analysisEx.getKey().cut();
                        analysisEx.remove(Key.GT);
                    } catch (PersistitException e) {
                        throw new PersistItErrorException (e);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    int itemNumber = 0;
                    for (final KeyCount keyCount : keyHistogram0.getSamples()) {
                        final byte[] bytes = keyCount.getBytes();
                        System.arraycopy(bytes, 0, key.getEncodedBytes(), 0,
                                bytes.length);
                        key.setEncodedSize(bytes.length);
                        key.indexTo(0);
                        int remainingSegments = key.getDepth();

                        if (index.isHKeyEquivalent()) {
                            IndexToHKey indexToHkey = index.indexToHKey();
                            for (int i = 0; i < indexToHkey.getLength(); ++i) {
                                final Object keySegmentValue = --remainingSegments >= 0 ? key .decode() : null;
                                if (!indexToHkey.isOrdinal(i)) {
                                    indexValues[indexToHkey.getFieldPosition(i)] = keySegmentValue;
                                }
                            }
                        } else {
                            for (final int field : indexDef.getFields()) {
                                if (--remainingSegments >= 0) {
                                    indexValues[field] = key.decode();
                                } else {
                                    indexValues[field] = null;
                                }
                            }
                        }
                        // Limit the toString() output to index fields
                        key.setEncodedSize(key.getIndex());

                        indexRowData.createRow(indexDef.getRowDef(),
                                indexValues);

                        final byte[] indexRowBytes = new byte[indexRowData
                                .getRowSize()];

                        System.arraycopy(indexRowData.getBytes(),
                                indexRowData.getRowStart(), indexRowBytes, 0,
                                indexRowData.getRowSize());

                        rowData.createRow(
                                indexAnalysisRowDef,
                                new Object[]{
                                        indexDef.getRowDef().getRowDefId(),
                                        index.getIndexId(), now, ++itemNumber,
                                        key.toString(), indexRowBytes,
                                        keyCount.getCount() * multiplier});
                        try {
                            store.writeRow(session, rowData);
                        } catch (InvalidOperationException e) {
                            throw new RollbackException(e);
                        }
                    }
                    //
                    // Add artificial end row containing all nulls.
                    //
                    indexRowData.createRow(indexDef.getRowDef(), new Object[0]);
                    final byte[] indexRowBytes = new byte[indexRowData
                            .getRowSize()];

                    System.arraycopy(indexRowData.getBytes(),
                            indexRowData.getRowStart(), indexRowBytes, 0,
                            indexRowData.getRowSize());

                    rowData.createRow(
                            indexAnalysisRowDef,
                            new Object[] { indexDef.getRowDef().getRowDefId(),
                                    index.getIndexId(), now, ++itemNumber,
                                    key.toString(), indexRowBytes,
                                    keyHistogram0.getKeyCount() * multiplier });

                    try {
                        store.writeRow(session, rowData);
                    } catch (InvalidOperationException e) {
                        throw new RollbackException(e);
                    }
                    treeService.getTableStatusCache().incrementRowCount(
                            indexAnalysisRowDef.getRowDefId());

                    transaction.commit(store.forceToDisk);
                    break;

                } catch (RollbackException re) {
                    if (--retries < 0) {
                        throw new TransactionFailedException();
                    }
                } finally {
                    transaction.end();
                }
            }
        } catch (RollbackException e) {
            Throwable cause = e.getCause();
            if (cause instanceof InvalidOperationException) {
                throw (InvalidOperationException) cause;
            }
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.akiban.server.store.IndexManager#populateTableStatistics(com.akiban
     * .server.service.session.Session, com.akiban.server.TableStatistics)
     */
    @Override
    public void populateTableStatistics(final Session session,
            final TableStatistics tableStatistics) throws PersistitException {
        final int tableId = tableStatistics.getRowDefId();
        final RowDef rowDef = store.getRowDefCache().getRowDef(tableId);
        if (rowDef == null) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Attempt to get table statistics "
                        + "for non-existent tableId " + tableId);
            }
            return;
        }

        for (Index index : rowDef.getIndexes()) {
            final Histogram histogram = new Histogram(index.getIndexId());
            final RowDef indexAnalysisRowDef = store.getRowDefCache().getRowDef(ANALYSIS_TABLE_NAME);
            final Exchange exchange = store.getExchange(session, indexAnalysisRowDef);
            exchange.clear().append(indexAnalysisRowDef.getOrdinal())
                    .append((long) tableId).append((long) index.getIndexId())
                    .append(Key.BEFORE);
            List<RowData> rows = new ArrayList<RowData>();
            while (exchange.next()) {
                final RowData rowData = new RowData(new byte[exchange
                        .getValue().getEncodedSize() + RowData.ENVELOPE_SIZE]);
                store.expandRowData(exchange, rowData);
                rows.add(rowData);
            }

            exchange.to(Key.BEFORE);
            for (final RowData rowData : rows) {
                final long rowCountLocation = indexAnalysisRowDef
                        .fieldLocation(rowData, ROW_COUNT_FIELD_INDEX);
                final long rowCount = rowData
                        .getIntegerValue((int) rowCountLocation,
                                (int) (rowCountLocation >>> 32));
                final long rowDataLocation = indexAnalysisRowDef.fieldLocation(
                        rowData, ROW_DATA_FIELD_INDEX);
                final int prefix = indexAnalysisRowDef.getFieldDef(
                        ROW_DATA_FIELD_INDEX).getPrefixSize();
                final RowData indexRowData = new RowData(
                        new byte[(int) (rowDataLocation >>> 32) - prefix]);
                System.arraycopy(rowData.getBytes(), (int) rowDataLocation
                        + prefix, indexRowData.getBytes(), 0,
                        indexRowData.getBufferLength());
                indexRowData.prepareRow(0);
                histogram
                        .addSample(new HistogramSample(indexRowData, rowCount));
            }
            if (!histogram.getHistogramSamples().isEmpty()) {
                tableStatistics.addHistogram(histogram);
            }
        }
    }
}
