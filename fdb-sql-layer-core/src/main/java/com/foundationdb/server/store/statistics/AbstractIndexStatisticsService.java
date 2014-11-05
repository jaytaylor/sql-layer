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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableIndex;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBSchemaManager;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.server.store.format.FDBStorageDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.WeakHashMap;

public abstract class AbstractIndexStatisticsService implements IndexStatisticsService, Service, TableListener
{
    private static final Logger log = LoggerFactory.getLogger(AbstractIndexStatisticsService.class);

    private static final int INDEX_STATISTICS_TABLE_VERSION = 1;
    private static final String BUCKET_COUNT_PROPERTY = "fdbsql.index_statistics.bucket_count";
    private static final String BUCKET_TIME_PROPERTY = "fdbsql.index_statistics.time_limit";
    private static final String BACKGROUND_TIME_PROPERTY = "fdbsql.index_statistics.background";
    private static final long TIME_LIMIT_UNLIMITED = -1;
    private static final long TIME_LIMIT_DISABLED = -2;

    protected final Store store;
    protected final TransactionService txnService;
    // Following couple only used by JMX method, where there is no context.
    protected final SchemaManager schemaManager;
    protected final SessionService sessionService;
    protected final ConfigurationService configurationService;
    protected final ListenerService listenerService;

    private AbstractStoreIndexStatistics storeStats;
    private Map<Index,IndexStatistics> cache;
    private BackgroundState backgroundState;
    private int bucketCount;
    private long scanTimeLimit, sleepTime, backgroundTimeLimit, backgroundSleepTime;

    protected AbstractIndexStatisticsService(Store store,
                                             TransactionService txnService,
                                             SchemaManager schemaManager,
                                             SessionService sessionService,
                                             ConfigurationService configurationService,
                                             ListenerService listenerService) {
        this.store = store;
        this.txnService = txnService;
        this.schemaManager = schemaManager;
        this.sessionService = sessionService;
        this.configurationService = configurationService;
        this.listenerService = listenerService;
    }

    protected abstract AbstractStoreIndexStatistics createStoreIndexStatistics();


    //
    // Service
    //

    @Override
    public void start() {
        cache = Collections.synchronizedMap(new WeakHashMap<Index,IndexStatistics>());
        storeStats = createStoreIndexStatistics();
        bucketCount = Integer.parseInt(configurationService.getProperty(BUCKET_COUNT_PROPERTY));
        parseTimeLimit(BUCKET_TIME_PROPERTY, false);
        parseTimeLimit(BACKGROUND_TIME_PROPERTY, true);
        registerStatsTables();
        listenerService.registerTableListener(this);
        backgroundState = new BackgroundState(backgroundTimeLimit != TIME_LIMIT_DISABLED);
    }

    private void parseTimeLimit(String key, boolean background) {
        String time = configurationService.getProperty(key);
        long on, sleep;
        if ("disabled".equals(time)) {
            on = TIME_LIMIT_DISABLED;
            sleep = 0;
        }
        else if ("unlimited".equals(time)) {
            on = TIME_LIMIT_UNLIMITED;
            sleep = 0;
        }
        else {
            int idx = time.indexOf(',');
            if (idx < 0) {
                on = Long.parseLong(time);
                sleep = 0;
            }
            else {
                on = Long.parseLong(time.substring(0, idx));
                sleep = Long.parseLong(time.substring(idx+1));
            }
        }
        if (background) {
            backgroundTimeLimit = on;
            backgroundSleepTime = sleep;
        }
        else {
            scanTimeLimit = on;
            sleepTime = sleep;
        }
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
        cache = null;
        storeStats = null;
        bucketCount = 0;
        backgroundState.stop();
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // IndexStatisticsService
    //

    @Override
    public IndexStatistics getIndexStatistics(Session session, Index index) {
        // TODO: Use getAnalysisTimestamp() of -1 to mark an "empty"
        // analysis to save going to disk for the same index every
        // time. Should this be a part of the IndexStatistics contract
        // somehow?
        IndexStatistics result = cache.get(index);
        if (result != null) {
            if (result.isInvalid())
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
        result.setValidity(IndexStatistics.Validity.INVALID);
        cache.put(index, result);
        return null;
    }

    @Override
    public void updateIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        if(indexes.isEmpty()) {
            return;
        }
        final Map<Index,IndexStatistics> updates = updateIndexStatistics(session, indexes, false);
        txnService.addCallback(session, TransactionService.CallbackType.COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                cache.putAll(updates);
                backgroundState.removeAll(updates);
            }
        });
    }

    private Map<Index,IndexStatistics> updateIndexStatistics(Session session,
                                                             Collection<? extends Index> indexes,
                                                             boolean background) {
        Map<Index,IndexStatistics> updates = new HashMap<>(indexes.size());
        long on, sleep;
        if (background) {
            on = backgroundTimeLimit;
            sleep = backgroundSleepTime;
        }
        else {
            on = scanTimeLimit;
            sleep = sleepTime;
        }
        for (Index index : indexes) {
            assert !index.leafMostTable().hasMemoryTableFactory() : index;
            IndexStatistics indexStatistics = storeStats.computeIndexStatistics(session, index, on, sleep);
            storeStats.storeIndexStatistics(session, index, indexStatistics);
            updates.put(index, indexStatistics);
        }
        return updates;
    }

    @Override
    public void deleteIndexStatistics(Session session, 
                                      Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            storeStats.removeStatistics(session, index);
            cache.remove(index);
        }
    }

    @Override
    public void loadIndexStatistics(Session session, 
                                    String schema, File file) throws IOException {
        AkibanInformationSchema ais = schemaManager.getAis(session);
        Map<Index,IndexStatistics> stats = new IndexStatisticsYamlLoader(ais, schema, store).load(file);
        for (Map.Entry<Index,IndexStatistics> entry : stats.entrySet()) {
            Index index = entry.getKey();
            IndexStatistics indexStatistics = entry.getValue();
            storeStats.storeIndexStatistics(session, index, indexStatistics);
            cache.put(index, indexStatistics);
            backgroundState.remove(index);
        }
    }

    @Override
    public void dumpIndexStatistics(Session session, 
                                    String schema, Writer file) throws IOException {
        Collection<Index> indexes = indexesInSchema(session, schema);
        // Get all the stats already computed for an index on this schema.
        Map<Index,IndexStatistics> toDump = new TreeMap<>(IndexStatisticsYamlLoader.INDEX_NAME_COMPARATOR);
        for (Index index : indexes) {
            IndexStatistics stats = getIndexStatistics(session, index);
            if (stats != null) {
                toDump.put(index, stats);
            }
        }
        new IndexStatisticsYamlLoader(schemaManager.getAis(session), schema, store).dump(toDump, file);
    }

    @Override
    public void deleteIndexStatistics(Session session, String schema) {
        deleteIndexStatistics(session, indexesInSchema(session, schema));
    }

    @Override
    public void clearCache() {
        cache.clear();
    }

    @Override
    public int bucketCount() {
        return bucketCount;
    }

    @Override
    public void missingStats(Session session, Index index, Column column) {
        if (index == null) {
            log.warn("No statistics for {}.{}; cost estimates will not be accurate", column.getTable().getName(), column.getName());
        }
        else {
            IndexStatistics stats = cache.get(index);
            if ((stats != null) && stats.isInvalid() && !stats.isWarned()) {
                if (index.isTableIndex()) {
                    Table table = ((TableIndex)index).getTable();
                    log.warn("No statistics for table {}; cost estimates will not be accurate", table.getName());
                    stats.setWarned(true);
                    backgroundState.offer(table);
                }
                else {
                    log.warn("No statistics for index {}; cost estimates will not be accurate", index.getIndexName());
                    stats.setWarned(true);
                    backgroundState.offer(index);
                }
            }
        }
    }

    public static final double MIN_ROW_COUNT_SMALLER = 0.2;
    public static final double MAX_ROW_COUNT_LARGER = 5.0;

    @Override
    public void checkRowCountChanged(Session session, Table table,
                                     IndexStatistics stats, long rowCount) {
        if (stats.isValid() && !stats.isWarned()) {
            double ratio = (double)Math.max(rowCount, 1) /
                           (double)Math.max(stats.getRowCount(), 1);
            String msg = null;
            long change = 1;
            if (ratio < MIN_ROW_COUNT_SMALLER) {
                msg = "smaller";
                change = Math.round(1.0 / ratio);
            }
            else if (ratio > MAX_ROW_COUNT_LARGER) {
                msg = "larger";
                change = Math.round(ratio);
            }
            if (msg != null) {
                stats.setValidity(IndexStatistics.Validity.OUTDATED);
                log.warn("Table {} is {} times {} than on {}; cost estimates will not be accurate until statistics are updated", new Object[] { table.getName(), change, msg, new Date(stats.getAnalysisTimestamp()) });
                stats.setWarned(true);
                backgroundState.offer(table);
            }
        }
    }

    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
        // None
    }

    @Override
    public void onDrop(Session session, Table table) {
        deleteIndexStatistics(session, table.getIndexesIncludingInternal());
        deleteIndexStatistics(session, table.getGroupIndexes());
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
        onDrop(session, table);
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        // None
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
        deleteIndexStatistics(session, indexes);
    }


    //
    // Internal
    //

    private static AkibanInformationSchema createStatsTables(SchemaManager schemaManager) {
        NewAISBuilder builder = AISBBasedBuilder.create(INDEX_STATISTICS_TABLE_NAME.getSchemaName(),
                                                        schemaManager.getTypesTranslator());
        builder.table(INDEX_STATISTICS_TABLE_NAME.getTableName())
                .colBigInt("table_id", false)
                .colBigInt("index_id", false)
                .colSystemTimestamp("analysis_timestamp", true)
                .colBigInt("row_count", true)
                .colBigInt("sampled_count", true)
                .pk("table_id", "index_id");
        builder.table(INDEX_STATISTICS_ENTRY_TABLE_NAME.getTableName())
                .colBigInt("table_id", false)
                .colBigInt("index_id", false)
                .colInt("column_count", false)
                .colInt("item_number", false)
                .colString("key_string", 2048, true, "latin1")
                .colVarBinary("key_bytes", 4096, true)
                .colBigInt("eq_count", true)
                .colBigInt("lt_count", true)
                .colBigInt("distinct_count", true)
                .pk("table_id", "index_id", "column_count", "item_number")
                .joinTo(INDEX_STATISTICS_TABLE_NAME.getSchemaName(), INDEX_STATISTICS_TABLE_NAME.getTableName(), "fk_0")
                .on("table_id", "table_id")
                .and("index_id", "index_id");

        // Statistics service relies on decoding rowdata manually
        if (schemaManager instanceof FDBSchemaManager) {
            Group istn = builder.unvalidatedAIS().getTable(INDEX_STATISTICS_TABLE_NAME).getGroup();
            istn.setStorageDescription(new FDBStorageDescription(istn, "rowdata"));

            Collection<TableIndex> collection = builder.unvalidatedAIS().getTable(INDEX_STATISTICS_TABLE_NAME).getIndexes();
            for (TableIndex ti : collection) {
                ti.setStorageDescription(new FDBStorageDescription(ti, "rowdata"));
            }

            Group isetn = builder.unvalidatedAIS().getTable(INDEX_STATISTICS_ENTRY_TABLE_NAME).getGroup();
            isetn.setStorageDescription(new FDBStorageDescription(isetn, "rowdata"));

            Collection<TableIndex> collection_isetn = builder.unvalidatedAIS().getTable(INDEX_STATISTICS_ENTRY_TABLE_NAME).getIndexes();
            for (TableIndex ti : collection_isetn) {
                ti.setStorageDescription(new FDBStorageDescription(ti, "rowdata"));
            }
        }
        
        builder.procedure(TableName.SYS_SCHEMA, "index_stats_delete")
               .language("java", Routine.CallingConvention.JAVA)
               .paramStringIn("schema_name", 128)
               .externalName(IndexStatisticsRoutines.class.getCanonicalName(), "delete");
        builder.procedure(TableName.SYS_SCHEMA, "index_stats_dump_file")
               .language("java", Routine.CallingConvention.JAVA)
               .paramStringIn("schema_name", 128)
               .paramStringIn("file_name", 4096)
               .returnString("file_path", 4096)
               .externalName(IndexStatisticsRoutines.class.getCanonicalName(), "dumpToFile");
        builder.procedure(TableName.SYS_SCHEMA, "index_stats_dump_string")
               .language("java", Routine.CallingConvention.JAVA)
               .paramStringIn("schema_name", 128)
               .returnString("yaml", 1048576)
               .externalName(IndexStatisticsRoutines.class.getCanonicalName(), "dumpToString");
        builder.procedure(TableName.SYS_SCHEMA, "index_stats_load_file")
               .language("java", Routine.CallingConvention.JAVA)
               .paramStringIn("schema_name", 128)
               .paramStringIn("file_name", 4096)
               .externalName(IndexStatisticsRoutines.class.getCanonicalName(), "loadFromFile");

        return builder.ais(true);
    }

    private void registerStatsTables() {
        AkibanInformationSchema ais = createStatsTables(schemaManager);
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(INDEX_STATISTICS_TABLE_NAME), INDEX_STATISTICS_TABLE_VERSION);
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(INDEX_STATISTICS_ENTRY_TABLE_NAME), INDEX_STATISTICS_TABLE_VERSION);

        for(Routine routine : ais.getRoutines().values()) {
            schemaManager.registerSystemRoutine(routine);
        }
    }

    private Collection<Index> indexesInSchema(Session session, String schema) {
        Set<Index> indexes = new HashSet<>();
        AkibanInformationSchema ais = schemaManager.getAis(session);
        for(Table t : ais.getSchema(schema).getTables().values()) {
            indexes.addAll(t.getIndexes());
            indexes.addAll(t.getGroup().getIndexes());
        }
        return indexes;
    }

    class BackgroundState implements Runnable {
        private final Queue<IndexName> queue = new ArrayDeque<>();
        private boolean active;
        private Thread thread = null;

        public BackgroundState(boolean active) {
            this.active = active;
        }

        public synchronized void offer(Table table) {
            for (Index index : table.getIndexes()) {
                offer(index);
            }
        }

        public synchronized void offer(Index index) {
            if (active) {
                IndexName entry = index.getIndexName();
                if (!queue.contains(entry)) {
                    if (queue.offer(entry)) {
                        if (thread == null) {
                            thread = new Thread(this, "IndexStatistics-Background");
                            thread.start();
                        }
                    }
                }
            }
        }

        public synchronized void removeAll(Map<Index,IndexStatistics> updates) {
            for (Index index : updates.keySet()) {
                remove(index);
            }
        }

        public synchronized void remove(Index index) {
            queue.remove(index.getIndexName());
        }

        public synchronized void stop() {
            active = false;
            if (thread != null) {
                thread.interrupt();
                try {
                    thread.join(1000); // Wait a little for it to shut down.
                }
                catch (InterruptedException ex) {
                    // Ignore
                }
            }
        }

        @Override
        public void run() {
            try (Session session = sessionService.createSession()) {
                while (active) {
                    IndexName entry;
                    synchronized (this) {
                        entry = queue.poll();
                        if (entry == null) {
                            thread = null;
                            break;
                        }
                    }
                    updateIndex(session, entry);
                }
            }
            catch (Exception ex) {
                log.warn("Error in background", ex);
                // TODO: Disable background altogether by turning off active?
                synchronized (this) {
                    queue.clear();
                    thread = null;
                }
            }
        }

        private void updateIndex(Session session, IndexName indexName) {
            Map<Index,IndexStatistics> statistics;
            try (TransactionService.CloseableTransaction txn = txnService.beginCloseableTransaction(session)) {
                Index index = null;
                AkibanInformationSchema ais = schemaManager.getAis(session);
                Table table = ais.getTable(indexName.getFullTableName());
                if (table != null)
                    index = table.getIndex(indexName.getName());
                if (index == null) {
                    Group group = ais.getGroup(indexName.getFullTableName());
                    if (group != null)
                        index = group.getIndex(indexName.getName());
                }
                if (index == null) return; // Could have been dropped in the meantime.
                statistics = updateIndexStatistics(session, Collections.singletonList(index), true);
                txn.commit();
                log.info("Automatically updated statistics for {}", indexName);
            }
            cache.putAll(statistics);
        }
    }
}
