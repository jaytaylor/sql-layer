package com.akiban.cserver.store;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.akiban.cserver.IndexDef;
import com.akiban.cserver.RowData;
import com.akiban.cserver.RowDef;
import com.akiban.cserver.TableStatistics;
import com.akiban.cserver.TableStatistics.Histogram;
import com.akiban.cserver.TableStatistics.HistogramSample;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyHistogram;
import com.persistit.Persistit;
import com.persistit.Transaction;
import com.persistit.TransactionRunnable;
import com.persistit.KeyHistogram.KeyCount;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

public class PersistitStoreIndexManager {

    private static final Log LOG = LogFactory
            .getLog(PersistitStoreIndexManager.class.getName());

    private final static String ANALYSIS_TABLE_NAME = "akiba_information_schema.index_analysis";

    private final static int DEFAULT_SAMPLE_SIZE = 32;

    private final static int STARTING_TREE_DEPTH = 2;

    /**
     * Field number in index_analysis table. Change if akiba_inormation_schema
     * changes.
     */
    private final static int ROW_COUNT_FIELD_INDEX = 6;

    /**
     * Field number in index_analysis table. Change if akiba_inormation_schema
     * changes.
     */
    private final static int ROW_DATA_FIELD_INDEX = 5;

    private final static int ROW_DATA_LENGTH = 4096;

    private final static int INDEX_LEVEL_MULTIPLIER = 200;

    private final static int SAMPLE_SIZE_MULTIPLIER = 32;

    // TODO - remove this once the ASE can handle returned
    // histograms without crashing. This is a temporary hack
    // for unit testing.
    static boolean enableHistograms = true;

    private final PersistitStore store;

    public PersistitStoreIndexManager(final PersistitStore store) {
        this.store = store;
    }

    public void startUp() {
    }

    public void shutDown() {
    }

    public void analyzeTable(final RowDef rowDef) throws Exception {
        analyzeTable(rowDef, DEFAULT_SAMPLE_SIZE - 1);
    }

    public void analyzeTable(final RowDef rowDef, final int sampleSize)
            throws Exception {
        for (final IndexDef indexDef : rowDef.getIndexDefs()) {
            analyzeIndex(indexDef, sampleSize);
        }
    }

    public void analyzeIndex(final IndexDef indexDef, final int sampleSize)
            throws Exception {
        final Exchange iEx = store.getExchange(indexDef.getRowDef(), indexDef);

        // First try to enumerate the values. If there are more than
        KeyHistogram keyHistogram = null;
        final int keyDepth = indexDef.getFields().length;
        int treeLevel = Math.max(0, iEx.getTree().getDepth()
                - STARTING_TREE_DEPTH);
        while (treeLevel >= 0) {
            keyHistogram = iEx.computeHistogram(Key.LEFT_GUARD_KEY,
                    Key.RIGHT_GUARD_KEY, sampleSize, keyDepth, treeLevel);
            if (keyHistogram.getKeyCount() > sampleSize
                    * SAMPLE_SIZE_MULTIPLIER) {
                break;
            }
            treeLevel = keyHistogram.getTreeDepth() - 1;
        }

        if (LOG.isInfoEnabled()) {
            LOG
                    .info(String
                            .format(
                                    "Analyzed index %s in table %s: %,d keys at keyDepth/treeLevel %d/%d",
                                    indexDef.getName(), indexDef.getRowDef()
                                            .getTableName(), keyHistogram
                                            .getKeyCount(), keyDepth, Math.max(
                                            0, treeLevel)));
        }

        final RowDef indexAnalysisRowDef = store.getRowDefCache().getRowDef(
                ANALYSIS_TABLE_NAME);
        final Exchange exchange = store.getExchange(indexAnalysisRowDef, null);
        final Transaction transaction = exchange.getTransaction();
        final Date now = new Date();
        final KeyHistogram keyHistogram0 = keyHistogram;
        final int multiplier = keyHistogram.getTreeDepth() == 1 ? INDEX_LEVEL_MULTIPLIER
                : 1;
        final Key key = new Key((Persistit) null);
        final RowData rowData = new RowData(new byte[ROW_DATA_LENGTH]);
        final RowData indexRowData = new RowData(new byte[ROW_DATA_LENGTH]);
        final Object[] indexValues = new Object[indexDef.getRowDef()
                .getFieldCount()];

        transaction.run(new TransactionRunnable() {

            @Override
            public void runTransaction() throws PersistitException,
                    RollbackException {

                rowData.createRow(indexAnalysisRowDef, new Object[] {
                        indexDef.getRowDef().getRowDefId(), indexDef.getId(),
                        now, 0, "", null, 0 });
                //
                // Remove previous analysis
                //
                try {
                    store.constructHKey(exchange, indexAnalysisRowDef, rowData);
                    exchange.getKey().cut();
                    exchange.remove(Key.GT);
                } catch (PersistitException e) {
                    throw e;
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
                    for (final int field : indexDef.getFields()) {
                        if (--remainingSegments >= 0) {
                            indexValues[field] = key.decode();
                        } else {
                            indexValues[field] = null;
                        }
                    }

                    // Limit the toString() output to index fields
                    key.setEncodedSize(key.getIndex());

                    indexRowData.createRow(indexDef.getRowDef(), indexValues);

                    final byte[] indexRowBytes = new byte[indexRowData
                            .getRowSize()];

                    System.arraycopy(indexRowData.getBytes(), indexRowData
                            .getRowStart(), indexRowBytes, 0, indexRowData
                            .getRowSize());

                    rowData.createRow(indexAnalysisRowDef, new Object[] {
                            indexDef.getRowDef().getRowDefId(),
                            indexDef.getId(), now, ++itemNumber,
                            key.toString(), indexRowBytes,
                            keyCount.getCount() * multiplier });

                    store.writeRow(rowData);
                }
                //
                // Add artificial end row containing all nulls.
                //
                indexRowData.createRow(indexDef.getRowDef(), new Object[0]);
                final byte[] indexRowBytes = new byte[indexRowData.getRowSize()];

                System.arraycopy(indexRowData.getBytes(), indexRowData
                        .getRowStart(), indexRowBytes, 0, indexRowData
                        .getRowSize());

                rowData.createRow(indexAnalysisRowDef, new Object[] {
                        indexDef.getRowDef().getRowDefId(), indexDef.getId(),
                        now, ++itemNumber, key.toString(), indexRowBytes,
                        keyHistogram0.getKeyCount() * multiplier });

                store.writeRow(rowData);
            }
        }, 10, 100, false);
    }

    public void populateTableStatistics(final TableStatistics tableStatistics)
            throws Exception {
        final int tableId = tableStatistics.getRowDefId();
        final RowDef rowDef = store.getRowDefCache().getRowDef(tableId);
        if (rowDef == null) {
            if (LOG.isWarnEnabled()) {
                LOG.warn("Attempt to get table statistics "
                        + "for non-existent tableId " + tableId);
            }
            return;
        }

        for (final IndexDef indexDef : rowDef.getIndexDefs()) {
            final Histogram histogram = new Histogram(indexDef.getId());
            final RowDef indexAnalysisRowDef = store.getRowDefCache()
                    .getRowDef(ANALYSIS_TABLE_NAME);
            final Exchange exchange = store.getExchange(indexAnalysisRowDef,
                    null);
            exchange.clear().append(indexAnalysisRowDef.getOrdinal()).append(
                    (long) tableId).append((long) indexDef.getId()).append(
                    Key.BEFORE);
            List<RowData> rows = new ArrayList<RowData>();
            while (exchange.next()) {
                final RowData rowData = new RowData(new byte[exchange
                        .getValue().getEncodedSize()
                        + RowData.ENVELOPE_SIZE]);
                store.expandRowData(exchange,
                        indexAnalysisRowDef.getRowDefId(), rowData);
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
                        + prefix, indexRowData.getBytes(), 0, indexRowData
                        .getBufferLength());
                indexRowData.prepareRow(0);
                histogram
                        .addSample(new HistogramSample(indexRowData, rowCount));
            }
            // TODO - remove the enableHistograms flag when Tom is ready
            if (enableHistograms && !histogram.getHistogramSamples().isEmpty()) {
                tableStatistics.addHistogram(histogram);
            }
        }
    }
}
