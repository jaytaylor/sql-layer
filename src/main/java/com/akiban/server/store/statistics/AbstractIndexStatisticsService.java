/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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

package com.akiban.server.store.statistics;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.GroupIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.TableIndex;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.jmx.JmxManageable;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;

public abstract class AbstractIndexStatisticsService implements IndexStatisticsService, Service, JmxManageable {
    private static final Logger log = LoggerFactory.getLogger(AbstractIndexStatisticsService.class);

    private static final int INDEX_STATISTICS_TABLE_VERSION = 1;
    private static final String BUCKET_COUNT_PROPERTY = "akserver.index_statistics.bucket_count";

    private final Store store;
    private final TreeService treeService;
    private final TransactionService txnService;
    // Following couple only used by JMX method, where there is no context.
    private final SchemaManager schemaManager;
    private final SessionService sessionService;
    private final ConfigurationService configurationService;

    private AbstractStoreIndexStatistics storeStats;
    private Map<Index,IndexStatistics> cache;
    private int bucketCount;

    protected AbstractIndexStatisticsService(Store store,
                                             TreeService treeService,
                                             TransactionService txnService,
                                             SchemaManager schemaManager,
                                             SessionService sessionService,
                                             ConfigurationService configurationService) {
        this.store = store;
        this.treeService = treeService;
        this.txnService = txnService;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
        this.configurationService = configurationService;
    }

    protected abstract AbstractStoreIndexStatistics createStoreIndexStatistics();
    protected abstract long countGIEntries(Session session, GroupIndex index);
    protected abstract long countGIEntriesApproximate(Session session, GroupIndex index);


    //
    // Service
    //

    @Override
    public void start() {
        store.setIndexStatistics(this);
        cache = Collections.synchronizedMap(new WeakHashMap<Index,IndexStatistics>());
        storeStats = createStoreIndexStatistics();
        bucketCount = Integer.parseInt(configurationService.getProperty(BUCKET_COUNT_PROPERTY));
        registerStatsTables();
    }

    @Override
    public void stop() {
        cache = null;
        storeStats = null;
        bucketCount = 0;
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // IndexStatisticsService
    //

    @Override
    public long countEntries(Session session, Index index) {
        switch(index.getIndexType()) {
            case TABLE: {
                final UserTable table = (UserTable)((TableIndex)index).getTable();
                if (table.hasMemoryTableFactory()) {
                    return table.getMemoryTableFactory().rowCount();
                } else {
                    return table.rowDef().getTableStatus().getRowCount();
                }
            }
            case GROUP:
                return countGIEntries(session, (GroupIndex)index);
            case FULL_TEXT:
                // TODO
                throw new UnsupportedOperationException("where is FT count?");
            default:
                throw new IllegalStateException("Unknown index type: " + index);
        }
    }

    @Override
    public long countEntriesApproximate(Session session, Index index) {
        switch(index.getIndexType()) {
            case TABLE: {
               final UserTable table = (UserTable)((TableIndex)index).getTable();
               return table.rowDef().getTableStatus().getApproximateRowCount();
            }
            case GROUP:
                return countGIEntriesApproximate(session, (GroupIndex)index);
            case FULL_TEXT:
                // TODO
                throw new UnsupportedOperationException("where is FT count?");
            default:
                throw new IllegalStateException("Unknown index type: " + index);
        }
    }

    @Override
    public long countEntriesManually(Session session, Index index) {
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
        result = storeStats.loadIndexStatistics(session, index);
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
        final Map<Index,IndexStatistics> updates = new HashMap<>(indexes.size());

        if (indexes.size() > 0) {
            final Index first = indexes.iterator().next();
            final UserTable table =  (UserTable)first.rootMostTable();
            if (table.hasMemoryTableFactory()) {
                updates.putAll(updateMemoryTableIndexStatistics(session, indexes));
            } else {
                updates.putAll(updateStoredTableIndexStatistics(session, indexes));
            }
        }
        txnService.addCallback(session, TransactionService.CallbackType.COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                cache.putAll(updates);
            }
        });
    }

    private Map<Index,IndexStatistics> updateStoredTableIndexStatistics(Session session,
                                                                        Collection<? extends Index> indexes) {
        Map<Index,IndexStatistics> updates = new HashMap<>(indexes.size());
        for (Index index : indexes) {
            IndexStatistics indexStatistics = storeStats.computeIndexStatistics(session, index);
            storeStats.storeIndexStatistics(session, index, indexStatistics);
            updates.put(index, indexStatistics);
        }
        return updates;
    }
    
    private Map<Index,IndexStatistics> updateMemoryTableIndexStatistics (Session session, Collection<? extends Index> indexes) {
        Map<Index,IndexStatistics> updates = new HashMap<>(indexes.size());
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
        for(Index index : indexes) {
            storeStats.removeStatistics(session, index);
            cache.remove(index);
        }
    }

    @Override
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException {
        ensureAdapter(session);
        AkibanInformationSchema ais = schemaManager.getAis(session);
        Map<Index,IndexStatistics> stats = new IndexStatisticsYamlLoader(ais, schema, treeService).load(file);
        for (Map.Entry<Index,IndexStatistics> entry : stats.entrySet()) {
            Index index = entry.getKey();
            IndexStatistics indexStatistics = entry.getValue();
            storeStats.storeIndexStatistics(session, index, indexStatistics);
            cache.put(index, indexStatistics);
        }
    }

    @Override
    public void dumpIndexStatistics(Session session, 
                                    String schema, Writer file) throws IOException {
        List<Index> indexes = new ArrayList<>();
        Set<Group> groups = new HashSet<>();
        AkibanInformationSchema ais = schemaManager.getAis(session);
        for (UserTable table : ais.getUserTables().values()) {
            if (table.getName().getSchemaName().equals(schema)) {
                indexes.addAll(table.getIndexes());
                if (groups.add(table.getGroup()))
                    indexes.addAll(table.getGroup().getIndexes());
            }
        }
        // Get all the stats already computed for an index on this schema.
        Map<Index,IndexStatistics> toDump = new TreeMap<>(IndexStatisticsYamlLoader.INDEX_NAME_COMPARATOR);
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

    @Override
    public int bucketCount() {
        return bucketCount;
    }


    //
    // JmxManageable
    //

    @Override
    public JmxObjectInfo getJmxObjectInfo() {
        return new JmxObjectInfo("IndexStatistics", 
                                 new JmxBean(), 
                                 IndexStatisticsMXBean.class);
    }

    private void ensureAdapter(Session session)
    {
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if(adapter == null) {
            adapter = store.createAdapter(session, SchemaCache.globalSchema(schemaManager.getAis(session)));
            session.put(StoreAdapter.STORE_ADAPTER_KEY, adapter);
        }
    }

    class JmxBean implements IndexStatisticsMXBean {
        @Override
        public String dumpIndexStatistics(String schema, String toFile) throws IOException {
            try(Session session = sessionService.createSession()) {
                File file = new File(toFile);
                try (FileWriter writer = new FileWriter(file)) {
                    dumpInternal(session, writer, schema);
                }
                return file.getAbsolutePath();
            }
        }

        @Override
        public String dumpIndexStatisticsToString(String schema) throws IOException {
            StringWriter writer = new StringWriter();
            try(Session session = sessionService.createSession()) {
                dumpInternal(session, writer, schema);
                writer.close();
                return writer.toString();
            }
        }

        @Override
        public void loadIndexStatistics(String schema, String fromFile)  throws IOException {
            try(Session session = sessionService.createSession()) {
                File file = new File(fromFile);
                try(TransactionService.CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
                    AbstractIndexStatisticsService.this.loadIndexStatistics(session, schema, file);
                    txn.commit();
                }
            } catch(RuntimeException ex) {
                log.error("Error loading " + schema, ex);
                throw ex;
            }
        }

        private void dumpInternal(Session session, Writer writer, String schema) throws IOException {
            try(TransactionService.CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
                AbstractIndexStatisticsService.this.dumpIndexStatistics(session, schema, writer);
                txn.commit();
            } catch(RuntimeException ex) {
                log.error("Error dumping " + schema, ex);
                throw ex;
            }
        }
    }


    //
    // Internal
    //

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
}
