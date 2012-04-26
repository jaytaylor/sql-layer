/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.store.statistics;

import com.akiban.ais.model.*;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.AccumulatorAdapter.AccumInfo;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.service.Service;
import com.akiban.server.service.dxl.DXLTransactionHook;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;

import com.google.inject.Inject;

import com.persistit.Exchange;
import com.persistit.Transaction;
import com.persistit.exception.PersistitException;
import com.persistit.exception.PersistitInterruptedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.*;
import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

public class IndexStatisticsServiceImpl implements IndexStatisticsService, Service<IndexStatisticsService>, JmxManageable
{
    private static final Logger log = LoggerFactory.getLogger(IndexStatisticsServiceImpl.class);

    private final PersistitStore store;
    private final TreeService treeService;
    // Following couple only used by JMX method, where there is no context.
    private final SchemaManager schemaManager;
    private final SessionService sessionService;

    private PersistitStoreIndexStatistics storeStats;
    private Map<Index,IndexStatistics> cache;

    @Inject
    public IndexStatisticsServiceImpl(Store store, TreeService treeService,
                                      SchemaManager schemaManager, SessionService sessionService) {
        this.store = store.getPersistitStore();
        this.treeService = treeService;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
    }
    
    /* Service */

    @Override
    public IndexStatisticsService cast() {
        return this;
    }

    @Override
    public Class<IndexStatisticsService> castClass() {
        return IndexStatisticsService.class;
    }

    @Override
    public void start() {
        store.setIndexStatistics(this);
        cache = Collections.synchronizedMap(new WeakHashMap<Index,IndexStatistics>());
        storeStats = new PersistitStoreIndexStatistics(store, treeService, this);
    }

    @Override
    public void stop() {
        cache = null;
        storeStats = null;
    }

    @Override
    public void crash() {
        stop();
    }

    /* IndexStatisticsService */

    @Override
    public long countEntries(Session session, Index index) throws PersistitInterruptedException {
        if (index.isTableIndex()) {
            return store.getTableStatus(((TableIndex)index).getTable()).getRowCount();
        }
        final Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getSnapshot(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, ex.getTree());
        }
        finally {
            store.releaseExchange(session, ex);
        }
    }

    @Override
    public long countEntriesApproximate(Session session, Index index) {
        if (index.isTableIndex()) {
            return store.getTableStatus(((TableIndex)index).getTable()).getApproximateRowCount();
        }
        final Exchange ex = store.getExchange(session, index);
        try {
            return AccumulatorAdapter.getLiveValue(AccumulatorAdapter.AccumInfo.ROW_COUNT, treeService, ex.getTree());
        }
        finally {
            store.releaseExchange(session, ex);
        }
    }

    @Override
    public IndexStatistics getIndexStatistics(Session session, Index index) {
        // TODO: Use getAnalysisTimestamp() of -1 to mark an "empty"
        // analysis to save going to disk for the same index every
        // time. Should this be a part of the IndexStatistics contract
        // somehow?
        IndexStatistics result = cache.get(index);
        if (result != null) {
            if (result.getAnalysisTimestamp() < 0)
                return null;
            else
                return result;
        }
        try {
            result = storeStats.loadIndexStatistics(session, index);
        }
        catch (PersistitException ex) {
            throw new PersistitAdapterException(ex);
        }
        if (result != null) {
            cache.put(index, result);
            return result;
        }
        result = new IndexStatistics(index);
        result.setAnalysisTimestamp(-1);
        cache.put(index, result);
        return null;
    }

    @Override
    public void updateIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        final Map<Index,IndexStatistics> updates = new HashMap<Index, IndexStatistics>(indexes.size());
        for (Index index : indexes) {
            try {
                IndexStatistics indexStatistics = 
                    storeStats.computeIndexStatistics(session, index);
                if (indexStatistics != null) {
                    storeStats.storeIndexStatistics(session, index, indexStatistics);
                    updates.put(index, indexStatistics);
                }
            }
            catch (PersistitException ex) {
                log.error("error while analyzing " + index, ex);
                throw new PersistitAdapterException(ex);
            }
            catch (RuntimeException e) {
                log.error("error while analyzing " + index, e);
                throw e;
            }
            catch (Error e) {
                log.error("error while analyzing " + index, e);
                throw e;
            }
        }
        DXLTransactionHook.addCommitSuccessCallback(session, new Runnable() {
            @Override
            public void run() {
                cache.putAll(updates);
            }
        });
    }

    @Override
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        for (Index index : indexes) {
            try {
                storeStats.deleteIndexStatistics(session, index);
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
            cache.remove(index);
        }
    }

    @Override
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        Map<Index,IndexStatistics> stats = 
            new IndexStatisticsYamlLoader(ais, schema).load(file);
        for (Map.Entry<Index,IndexStatistics> entry : stats.entrySet()) {
            Index index = entry.getKey();
            IndexStatistics indexStatistics = entry.getValue();
            try {
                storeStats.storeIndexStatistics(session, index, indexStatistics);
            }
            catch (PersistitException ex) {
                throw new PersistitAdapterException(ex);
            }
            cache.put(index, indexStatistics);
        }
    }

    @Override
    public void dumpIndexStatistics(Session session, 
                                    String schema, Writer file) throws IOException {
        List<Index> indexes = new ArrayList<Index>();
        Set<Group> groups = new HashSet<Group>();
        AkibanInformationSchema ais = schemaManager.getAis(session);
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getName().getSchemaName().equals(schema)) {
                indexes.addAll(table.getIndexes());
                if (groups.add(table.getGroup()))
                    indexes.addAll(table.getGroup().getIndexes());
            }
        }
        // Get all the stats already computed for an index on this schema.
        Map<Index,IndexStatistics> toDump = new TreeMap<Index,IndexStatistics>(IndexStatisticsYamlLoader.INDEX_NAME_COMPARATOR);
        for (Index index : indexes) {
            IndexStatistics stats = getIndexStatistics(session, index);
            if (stats != null) {
                toDump.put(index, stats);
            }
        }
        new IndexStatisticsYamlLoader(ais, schema).dump(toDump, file);
    }

    @Override
    public void clearCache() {
        cache.clear();
    }

    /* JmxManageable */

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("IndexStatistics", 
                                 new JmxBean(), 
                                 IndexStatisticsMXBean.class);
    }

    private IndexCheckSummary checkAndFix(Session session, String schemaRegex, String tableRegex) {
        long startNs = System.nanoTime();
        Pattern schemaPattern = Pattern.compile(schemaRegex);
        Pattern tablePattern = Pattern.compile(tableRegex);
        List<IndexCheckResult> results = new ArrayList<IndexCheckResult>();
        AkibanInformationSchema ais = schemaManager.getAis(session);

        for (Map.Entry<TableName,UserTable> entry : ais.getUserTables().entrySet()) {
            TableName tName = entry.getKey();
            if (schemaPattern.matcher(tName.getSchemaName()).find()
                    && tablePattern.matcher(tName.getSchemaName()).find())
            {
                UserTable uTable = entry.getValue();
                List<Index> indexes = new ArrayList<Index>();
                indexes.add(uTable.getPrimaryKeyIncludingInternal().getIndex());
                for (Index gi : uTable.getGroup().getIndexes()) {
                    if (gi.leafMostTable().equals(uTable))
                        indexes.add(gi);
                }
                for (Index index : indexes) {
                    IndexCheckResult indexCheckResult = checkAndFixIndex(session, index);
                    results.add(indexCheckResult);
                }
            }
        }
        long endNs = System.nanoTime();
        return new IndexCheckSummary(results,  endNs - startNs);
    }

    private IndexCheckResult checkAndFixIndex(Session session, Index index) {
        Transaction txn = store.getDb().getTransaction();
        try {
            txn.begin();
        } catch (PersistitException e) {
            log.error("couldn't start transaction", e);
            throw new PersistitAdapterException(e);
        }
        try {
            long expected = countEntries(session, index);
            long actual = storeStats.manuallyCountEntries(session, index);
            if (expected != actual) {
                if (index.isTableIndex()) {
                    store.getTableStatus(((TableIndex)index).getTable()).setRowCount(actual);
                }
                else {
                    final Exchange ex = store.getExchange(session, index);
                    try {
                        AccumulatorAdapter accum =
                                new AccumulatorAdapter(AccumInfo.ROW_COUNT, treeService, ex.getTree());
                        accum.set(actual);
                    }
                    finally {
                        store.releaseExchange(session, ex);
                    }
                }
            }
            txn.commit();
            return new IndexCheckResult(index.getIndexName(), expected, actual, countEntries(session, index));
        }
        catch (Exception e) {
            log.error("while checking/fixing " + index, e);
            return new IndexCheckResult(index.getIndexName(), -1, -1, -1);
        }
        finally {
            txn.end();
        }
    }


    class JmxBean implements IndexStatisticsMXBean {
        @Override
        public String dumpIndexStatistics(String schema, String toFile) 
                throws IOException {
            Session session = sessionService.createSession();
            try {
                File file = new File(toFile);
                FileWriter writer = new FileWriter(file);
                try {
                    IndexStatisticsServiceImpl.this.dumpIndexStatistics(session, schema, writer);
                }
                finally {
                    writer.close();
                }
                return file.getAbsolutePath();
            }
            finally {
                session.close();
            }
        }

        @Override
        public String dumpIndexStatisticsToString(String schema) throws IOException {
            Session session = sessionService.createSession();
            try {
                StringWriter writer = new StringWriter();
                IndexStatisticsServiceImpl.this.dumpIndexStatistics(session, schema, writer);
                writer.close();
                return writer.toString();
            }
            finally {
                session.close();
            }
        }

        @Override
        public void loadIndexStatistics(String schema, String fromFile) 
                throws IOException {
            Session session = sessionService.createSession();
            try {
                File file = new File(fromFile);
                IndexStatisticsServiceImpl.this.loadIndexStatistics(session, schema, file);
            }
            finally {
                session.close();
            }
        }

        @Override
        public IndexCheckSummary checkAndFix(String schemaRegex, String tableRegex) {
            Session session = sessionService.createSession();
            try {
                return IndexStatisticsServiceImpl.this.checkAndFix(session, schemaRegex, tableRegex);
            }
            finally {
                session.close();
            }
        }

        @Override
        public IndexCheckSummary checkAndFixAll() {
            return checkAndFix(".*", ".*");
        }
    }
}
