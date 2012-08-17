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
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.AccumulatorAdapter;
import com.akiban.server.error.PersistitAdapterException;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
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

public class IndexStatisticsServiceImpl implements IndexStatisticsService, Service<IndexStatisticsService>, JmxManageable
{
    private final static int INDEX_STATISTICS_TABLE_VERSION = 1;

    private static final Logger log = LoggerFactory.getLogger(IndexStatisticsServiceImpl.class);

    private final PersistitStore store;
    private final TreeService treeService;
    // Following couple only used by JMX method, where there is no context.
    private final SchemaManager schemaManager;
    private final SessionService sessionService;
    private final ConfigurationService configurationService;

    private PersistitStoreIndexStatistics storeStats;
    private Map<Index,IndexStatistics> cache;

    @Inject
    public IndexStatisticsServiceImpl(Store store,
                                      TreeService treeService,
                                      SchemaManager schemaManager,
                                      SessionService sessionService,
                                      ConfigurationService configurationService) {
        this.store = store.getPersistitStore();
        this.treeService = treeService;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
        this.configurationService = configurationService;
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
        registerStatsTables();
    }

    private static AkibanInformationSchema createStatsTables() {
        NewAISBuilder builder = AISBBasedBuilder.create(INDEX_STATISTICS_TABLE_NAME.getSchemaName());
        builder.userTable(INDEX_STATISTICS_TABLE_NAME.getTableName())
                .colLong("table_id", false)
                .colLong("index_id", false)
                .colTimestamp("analysis_timestamp", true)
                .colBigInt("row_count", true)
                .colBigInt("sampled_count", true)
                .pk("table_id", "index_id");
        builder.userTable(INDEX_STATISTICS_ENTRY_TABLE_NAME.getTableName())
                .colLong("table_id", false)
                .colLong("index_id", false)
                .colLong("column_count", false)
                .colLong("item_number", false)
                .colString("key_string", 2048, true, "latin1")
                .colVarBinary("key_bytes", 4096, true)
                .colBigInt("eq_count", true)
                .colBigInt("lt_count", true)
                .colBigInt("distinct_count", true)
                .pk("table_id", "index_id", "column_count", "item_number")
                .joinTo(INDEX_STATISTICS_TABLE_NAME.getSchemaName(), INDEX_STATISTICS_TABLE_NAME.getTableName(), "fk_0")
                    .on("table_id", "table_id")
                    .and("index_id", "index_id");
        return builder.ais(true);
    }

    private void registerStatsTables() {
        AkibanInformationSchema ais = createStatsTables();
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(INDEX_STATISTICS_TABLE_NAME), INDEX_STATISTICS_TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(INDEX_STATISTICS_ENTRY_TABLE_NAME), INDEX_STATISTICS_TABLE_VERSION);
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
            final UserTable table = (UserTable)((TableIndex)index).getTable();
            if (table.hasMemoryTableFactory()) {
                return table.getMemoryTableFactory().rowCount();
            } else {
                return store.getTableStatus(table).getRowCount();
            }
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
    public long countEntriesManually(Session session, Index index) throws PersistitException {
        return storeStats.manuallyCountEntries(session, index);
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
        catch (PersistitInterruptedException ex) {
            throw new QueryCanceledException(session);
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
        ensureAdapter(session);
        final Map<Index,IndexStatistics> updates = new HashMap<Index, IndexStatistics> (indexes.size());

        if (indexes.size() > 0) {
            final Index first = indexes.iterator().next();
            final UserTable table =  (UserTable)first.rootMostTable();
            if (table.hasMemoryTableFactory()) {
                updates.putAll(updateMemoryTableIndexStatistics (session, indexes));
            } else {
                updates.putAll(updatePersistitTableIndexStatistics (session, indexes));
            }
        }        
        DXLTransactionHook.addCommitSuccessCallback(session, new Runnable() {
            @Override
            public void run() {
                cache.putAll(updates);
            }
        });
    }

    private Map<Index,IndexStatistics> updatePersistitTableIndexStatistics (Session session, Collection<? extends Index> indexes) {
        Map<Index,IndexStatistics> updates = new HashMap<Index, IndexStatistics>(indexes.size());
        for (Index index : indexes) {
            try {
                IndexStatistics indexStatistics = 
                    storeStats.computeIndexStatistics(session, index);
                if (indexStatistics != null) {
                    storeStats.storeIndexStatistics(session, index, indexStatistics);
                    updates.put(index, indexStatistics);
                }
            }
            catch (PersistitInterruptedException ex) {
                log.info("interrupt while analyzing " + index, ex);
                throw new QueryCanceledException(session);
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
        return updates;
    }
    
    private Map<Index,IndexStatistics> updateMemoryTableIndexStatistics (Session session, Collection<? extends Index> indexes) {
        Map<Index,IndexStatistics> updates = new HashMap<Index, IndexStatistics>(indexes.size());
        IndexStatistics indexStatistics;
        for (Index index : indexes) {
            // memory store, when it calculates index statistics, and supports group indexes
            // will work on the root table. 
            final UserTable table =  (UserTable)index.rootMostTable();
            indexStatistics = table.getMemoryTableFactory().computeIndexStatistics(session, index);

            if (indexStatistics != null) {
                updates.put(index, indexStatistics);
            }
        }
        return updates;
    }

    @Override
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        ensureAdapter(session);
        for (Index index : indexes) {
            try {
                storeStats.deleteIndexStatistics(session, index);
            }
            catch (PersistitInterruptedException ex) {
                throw new QueryCanceledException(session);
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
        ensureAdapter(session);
        AkibanInformationSchema ais = schemaManager.getAis(session);
        Map<Index,IndexStatistics> stats = 
            new IndexStatisticsYamlLoader(ais, schema, treeService).load(file);
        for (Map.Entry<Index,IndexStatistics> entry : stats.entrySet()) {
            Index index = entry.getKey();
            IndexStatistics indexStatistics = entry.getValue();
            try {
                storeStats.storeIndexStatistics(session, index, indexStatistics);
            }
            catch (PersistitInterruptedException ex) {
                throw new QueryCanceledException(session);
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
        new IndexStatisticsYamlLoader(ais, schema, treeService).dump(toDump, file);
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

    private void ensureAdapter(Session session)
    {
        PersistitAdapter adapter = (PersistitAdapter) session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null) {
            adapter = new PersistitAdapter(SchemaCache.globalSchema(schemaManager.getAis(session)),
                                           store,
                                           treeService,
                                           session,
                                           configurationService,
                                           true);
            session.put(StoreAdapter.STORE_ADAPTER_KEY, adapter);
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
            catch (RuntimeException ex) {
                log.error("Error dumping " + schema, ex);
                throw ex;
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
            catch (RuntimeException ex) {
                log.error("Error dumping " + schema, ex);
                throw ex;
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
            catch (RuntimeException ex) {
                log.error("Error loading " + schema, ex);
                throw ex;
            }
            finally {
                session.close();
            }
        }
    }
}
