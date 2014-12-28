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

package com.foundationdb.server.store;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.protobuf.ProtobufWriter.WriteSelector;
import com.foundationdb.qp.memoryadapter.BasicFactoryBase;
import com.foundationdb.qp.memoryadapter.MemoryAdapter;
import com.foundationdb.qp.memoryadapter.MemoryGroupCursor;
import com.foundationdb.qp.row.ValuesHolderRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.TableStatus;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.RowDefBuilder;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.security.SecurityService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.tree.TreeLink;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.service.tree.TreeVisitor;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.foundationdb.server.store.format.PersistitStorageFormatRegistry;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.server.util.ReadWriteMap;
import com.google.inject.Inject;
import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.Key.Direction;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;
import com.persistit.exception.RollbackException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.foundationdb.qp.storeadapter.PersistitAdapter.wrapPersistitException;
import static com.foundationdb.server.service.transaction.TransactionService.Callback;
import static com.foundationdb.server.service.transaction.TransactionService.CallbackType;

/**
 * Version 1.9.2 - 2013-10
 * <pre>
 *     - Data format version for non-SchemaManager (i.e. row/index) contents
 *     - Metadata format version for SchemaManager contents
 *     - Accumulator, at {@link #ACCUMULATOR_INDEX_SCHEMA_GEN}, for AIS generation
 *     - Serialized Protobuf per schema, via {@link ProtobufWriter}
 *     - Removed trees stored, with id and timestamp, for cleanup at next start
 *     - In-progress DDL, identified by session ID, stores two chunks of data:
 *         - Each affected table gets a key mapping to the DDL ID
 *         - New serialized Protobuf stored for each affected schema
 *
 *     {@link #SCHEMA_TREE_NAME}
 *         Accumulator[0]                           =>  Seq (generation)
 *         Accumulator[1]                           =>  Seq (online IDs)
 *         "metaVersion"                            =>  long
 *         "dataVersion"                            =>  long
 *         "delayed",(long)id,(long)ts              =>  "schema","tree"
 *         ...
 *         "protobuf","schema"                      =>  byte[] (AIS Protobuf)
 *         ...
 *         "protobufMem"                            =>  byte[] (AIS Protobuf)
 *         ...
 *         "online",(long)id                        =>  long (generation)
 *         ...
 *         "online",(long)id,"change",(int)tid      =>  byte[] (ChangeSet Protobuf)
 *         ...
 *         "online",(long)id,"error"                =>  String (error message, only set on error)
 *         ...
 *         "online",(long)id,"protobuf","schema"    =>  byte[] (AIS Protobuf)
 *         ...
 * </pre>
 *
 * Version 1.2.1 - 2012-05
 * <pre>
 *     - Accumulator, at {@link #ACCUMULATOR_INDEX_SCHEMA_GEN}, for AIS generation
 *     - Serialized Protobuf per schema, via {@link ProtobufWriter}
 *     - Single "version" stored with every schema entry
 *     - Removed trees stored, with id and timestamp, for cleanup at next start
 *
 *     {@link #SCHEMA_TREE_NAME}
 *         Accumulator[0]                       =>  Seq
 *         "delayedTree",(long)id,(long)ts      =>  "schema","treeName"
 *         ...
 *         "by","PBAIS",(int)ver,"schema"       =>  byte[]
 *         ...
 *         "by","PBMEMAIS"                      =>  byte[]
 * </pre>
 *
 * Version 0.4 - 2011-05
 * <pre>
 *     - Single k/v pair containing the entire serialized AIS, via the
 *       deleted com.foundationdb.ais.metamodel.io.MessageTarget.
 *
 *     {@link #SCHEMA_TREE_NAME}
 *         "byAIS"      =>  byte[]
 * </pre>
 */
public class PersistitStoreSchemaManager extends AbstractSchemaManager {
    private static enum GenValue { NEW, SNAPSHOT }
    private static enum GenMap { PUT_NEW, NO_PUT }

    private static class SharedAIS {
        public final AtomicInteger refCount;
        public final AkibanInformationSchema ais;

        public SharedAIS(AkibanInformationSchema ais) {
            this.refCount = new AtomicInteger(0);
            this.ais = ais;
        }

        public int acquire() {
            return refCount.incrementAndGet();
        }

        public int release() {
            int count = refCount.decrementAndGet();
            assert count >= 0 : count;
            return count;
        }

        public int shareCount() {
            return refCount.get();
        }

        public boolean isShared() {
            return refCount.get() > 0;
        }

        @Override
        public String toString() {
            return "Shared[" + refCount.get() + "]" + ais;
        }
    }

    private static class AISAndTimestamp {
        public final SharedAIS sAIS;
        public final long timestamp;

        public AISAndTimestamp(SharedAIS sAIS, long timestamp) {
            this.sAIS = sAIS;
            this.timestamp = timestamp;
        }

        public boolean isUsableForStartTime(long startTime) {
            return startTime > timestamp;
        }

        @Override
        public String toString() {
            return sAIS + ":" + timestamp;
        }
    }

    /**
     * 1 - initial
     * 2 - fix [var]char size calculation
     * 3 - remove string digesting from PKVT
     * 4 - unique index row format stored as regular indexes again
     * 5 - no more group index row counters
     */
    private static final long CURRENT_DATA_VERSION = 5;
    /**
     * 1 - initial
     * 2 - type bundles
     * 3 - Online DDL error-ing
     * 4 - ???
     * 5 - ???
     * 6 - hidden PK changed to Sequence/__row_id
     */
    private static final long CURRENT_META_VERSION = 6;
    private static final String S_K_META_VERSION = "metaVersion";
    private static final String S_K_DATA_VERSION = "dataVersion";
    private static final String S_K_DELAYED = "delayed";
    private static final String S_K_PROTOBUF = "protobuf";
    private static final String S_K_PROTOBUF_MEM = "protobufMem";
    private static final String S_K_ONLINE = "online";
    private static final String S_K_CHANGE = "change";
    private static final String S_K_ERROR = "error";
    private static final int ACCUMULATOR_INDEX_SCHEMA_GEN = 0;
    private static final int ACCUMULATOR_INDEX_ONLINE_ID = 1;

    /** Tree this class puts data in. */
    private final static String SCHEMA_TREE_NAME = "_schema_";
    /** Tree (includes online ID and table ID) concurrent DML during online DDL go in. */
    private final static String ONLINE_HANDLED_HKEY_TREE_FMT = "_schema_c%d_t%d";
    private static final Session.Key<SharedAIS> SESSION_SAIS_KEY = Session.Key.named("PSSM_SAIS");
    private final static Session.MapKey<Integer,Integer> TABLE_VERSIONS = Session.MapKey.mapNamed("TABLE_VERSIONS");

    /**
     * <p>This is used as unusable cache identifier, count for outstanding DDLs, and sync object for updating
     * the latest cache value.</p>
     * <p>Why are all needed? Consider the simple case two concurrent DDLs, A and B. Both are have pre-committed
     * and cleared the cache. If A commits, the cache is updated, and then B commits, any reader using the cache
     * based on timestamp alone will get a stale snapshot. So, the cache can *only* be updated when there are no
     * more outstanding.</p>
     */
    private static final AISAndTimestamp CACHE_SENTINEL = new AISAndTimestamp(new SharedAIS(null), Long.MAX_VALUE);

    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManager.class);

    private final TreeService treeService;
    private PersistitNameGenerator nameGenerator;
    private AtomicLong delayedTreeIDGenerator;
    private ReadWriteMap<Long,SharedAIS> aisMap;
    private volatile AISAndTimestamp latestAISCache;
    private TransactionService.Callback clearLatestCacheCallback;
    private TransactionService.Callback enqueueClearAndUpdateCallback;
    private BlockingQueue<QueueTask> taskQueue;
    private Thread queueConsumer;
    private AtomicInteger aisCacheMissCount;
    private AtomicInteger loadAISFromStorageCount;
    private AtomicInteger delayedTreeCount;
    private ReadWriteMap<Integer,Integer> tableVersionMap;

    @Inject
    public PersistitStoreSchemaManager(ConfigurationService config, SessionService sessionService,
                                       TreeService treeService, TransactionService txnService,
                                       TypesRegistryService typesRegistryService) {
        super(config, sessionService, txnService, typesRegistryService,
              new PersistitStorageFormatRegistry(config));
        this.treeService = treeService;
    }

    @Override
    protected NameGenerator getNameGenerator(Session session) {
        return nameGenerator;
    }

    @Override
    protected AkibanInformationSchema getSessionAIS(Session session) {
        return getSharedAIS(session).ais;
    }

    @Override
    public void setSecurityService(SecurityService securityService) {
        this.securityService = securityService;
    }

    @Override
    public void addOnlineHandledHKey(Session session, int tableID, Key hKey) {
        Exchange ex = onlineHandledHKeyExchange(session, tableID);
        try {
            ex.clear().getKey().appendByteArray(hKey.getEncodedBytes(), 0, hKey.getEncodedSize());
            ex.store();
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    public Iterator<byte[]> getOnlineHandledHKeyIterator(final Session session, int tableID, Key hKey) {
        final Exchange ex = onlineHandledHKeyExchange(session, tableID);
        if(hKey != null) {
            ex.clear().getKey().appendByteArray(hKey.getEncodedBytes(), 0, hKey.getEncodedSize());
        }
        return new Iterator<byte[]>() {
            private Key.Direction dir = Key.GTEQ;

            @Override
            public boolean hasNext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] next() {
                try {
                    if(!ex.traverse(dir, true)) {
                        return null;
                    }
                    dir = Direction.GT;
                    return ex.getKey().decodeByteArray();
                } catch(PersistitException | RollbackException e) {
                    throw wrapPersistitException(session, e);
                }
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public void setOnlineDMLError(Session session, int tableID, String message) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        Exchange ex = schemaTreeExchange(session);
        try {
            ex.clear().append(S_K_ONLINE).append(onlineSession.id).append(S_K_ERROR);
            ex.getValue().putString(message);
            ex.store();
        } catch(PersistitException | RollbackException e) {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
            throw wrapPersistitException(session, e);
        }
    }

    @Override
    public String getOnlineDMLError(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        Exchange ex = schemaTreeExchange(session);
        try {
            ex.clear().append(S_K_ONLINE).append(onlineSession.id).append(S_K_ERROR);
            ex.fetch();
            // write skew
            ex.lock();
            return ex.getValue().isDefined() ? ex.getValue().getString() : null;
        } catch(PersistitException | RollbackException e) {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
            throw wrapPersistitException(session, e);
        }
    }

    @Override
    public void addOnlineChangeSet(Session session, ChangeSet changeSet) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        onlineSession.tableIDs.add(changeSet.getTableId());
        Exchange ex = schemaTreeExchange(session);
        try {
            ex.clear().append(S_K_ONLINE).append(onlineSession.id).append(S_K_CHANGE).append(changeSet.getTableId());
            ex.getValue().putByteArray(ChangeSetHelper.save(changeSet));
            ex.store();
        } catch(PersistitException | RollbackException e) {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
            throw wrapPersistitException(session, e);
        }
    }

    @Override
    public void discardOnline(Session session) {
        // Need to restore any changed versions
        AkibanInformationSchema ais = getSessionAIS(session);
        tableVersionMap.claimExclusive();
        try {
            for(Integer tid : getOnlineSession(session, true).tableIDs) {
                int curVersion = ais.getTable(tid).getVersion();
                tableVersionMap.getWrappedMap().put(tid, curVersion);
            }
        } finally {
            tableVersionMap.releaseExclusive();
        }
        super.discardOnline(session);
    }

    @Override
    public Set<String> getTreeNames(Session session) {
        Set<String> treeNames = new TreeSet<>();
        treeNames.addAll(super.getTreeNames(session));
        treeNames.add(SCHEMA_TREE_NAME);
        return treeNames;
    }

    @Override
    public boolean hasTableChanged(Session session, int tableID) {
        AkibanInformationSchema ais = getAis(session);
        Table table = ais.getTable(tableID);
        final Integer tableVersion = (table != null) ? table.getVersion() : null;
        final Integer globalVersion = tableVersionMap.get(tableID);
        assert (globalVersion != null) : tableID;
        if(table != null) {
            assert (tableVersion != null) : table;
            // Normal: Current view matches global
            if(tableVersion.equals(globalVersion)) {
                return false;
            }
        }
        // else: DROP that scanned to get rid of rows and *must* from this session

        // DDL A: Ongoing change from *this* session
        Map<Integer,Integer> changedTables = session.get(TABLE_VERSIONS);
        if(changedTables != null) {
            Integer changedVersion = changedTables.get(tableID);
            if(tableVersion == null) {
                assert (changedVersion != null) : tableID;
                return false;
            }
            if(tableVersion.equals(changedVersion)) {
                return false;
            }
        }

        // DDL B: Ongoing change from *another* session that is still in progress.
        //        If global matches online view then it has been handled correctly.
        OnlineCache cache = getOnlineCache(session, ais);
        Long onlineID = cache.tableToOnline.get(tableID);
        if(onlineID != null) {
            AkibanInformationSchema onlineAIS = cache.onlineToAIS.get(onlineID);
            Table onlineTable = onlineAIS.getTable(tableID);
            Integer onlineVersion = (onlineTable != null) ? onlineTable.getVersion() : null;
            if(globalVersion.equals(onlineVersion)) {
                return false;
            }
        }

        // Otherwise the table has changed since session's transaction started.
        return true;
    }

    private SharedAIS getSharedAIS(Session session) {
        SharedAIS local = session.get(SESSION_SAIS_KEY);
        if(local != null) {
            return local;
        }

        // Latest is a volatile read and that is all that is required. If it is a valid cache, it can only
        // transition to non-valid (pre-committed DDL that doesn't affect out read validity) or the same
        // snapshot with a higher timestamp. See checks in updateLatestAISCache.
        final AISAndTimestamp cached = latestAISCache;
        final long startTimestamp = txnService.getTransactionStartTimestamp(session);
        if(cached.isUsableForStartTime(startTimestamp)) {
            local = cached.sAIS;
        }

        // Couldn't use the cache, do an always accurate accumulator lookup and check in map.
        long generation = 0;
        if(local == null) {
            aisCacheMissCount.incrementAndGet();
            generation = getGenerationSnapshot(session);
            local = aisMap.get(generation);
        }

        // Wasn't in map so need to reload from disk.
        // Should be 1) very rare and 2) fairly quick, so just do it under write lock to avoid duplicate work/entries
        if(local == null) {
            aisMap.claimExclusive();
            try {
                // Double check while while under exclusive
                local = aisMap.get(generation);
                if(local == null) {
                    loadAISFromStorageCount.incrementAndGet();
                    try {
                        local = loadToShared(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);
                        buildRowDefs(session, local.ais);
                    } catch(PersistitException | RollbackException e) {
                        throw wrapPersistitException(session, e);
                    }
                }
            } finally {
                aisMap.releaseExclusive();
            }
        }

        attachToSession(session, local);
        return local;
    }

    void treeWasRemoved(Session session, String schema, String treeName) {
        LOG.debug("Delaying removal of tree (until next restart): {}", treeName);
        delayedTreeCount.incrementAndGet();
        Exchange ex = schemaTreeExchange(session);
        try {
            long id = delayedTreeIDGenerator.incrementAndGet();
            long timestamp = System.currentTimeMillis();
            ex.clear().append(S_K_DELAYED).append(id).append(timestamp);
            ex.getValue().setStreamMode(true);
            ex.getValue().put(schema);
            ex.getValue().put(treeName);
            ex.getValue().setStreamMode(false);
            ex.store();
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    public void start() {
        super.start();

        boolean skipAISUpgrade = Boolean.parseBoolean(config.getProperty(SKIP_AIS_UPGRADE_PROPERTY));

        this.tableVersionMap = ReadWriteMap.wrapNonFair(new HashMap<Integer,Integer>());
        this.aisMap = ReadWriteMap.wrapNonFair(new HashMap<Long,SharedAIS>());

        final AkibanInformationSchema newAIS;
        try(Session session = sessionService.createSession()) {
            newAIS = txnService.run(session, new Callable<AkibanInformationSchema>() {
                @Override
                public AkibanInformationSchema call() {
                    try {
                        // Unrelated to loading, but fine time to do it
                        cleanupDelayedTrees(session, true);

                        SharedAIS sAIS = loadToShared(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);

                        buildRowDefs(session, sAIS.ais);

                        long startTimestamp = txnService.getTransactionStartTimestamp(session);
                        sAIS.acquire(); // So count while in cache is 1
                        latestAISCache = new AISAndTimestamp(sAIS, startTimestamp);
                        return sAIS.ais;
                    } catch(PersistitException e) {
                        throw wrapPersistitException(session, e);
                    }
                }
            });
        }

        this.nameGenerator = new PersistitNameGenerator(newAIS);
        this.delayedTreeIDGenerator = new AtomicLong();
        for(Table table : newAIS.getTables().values()) {
            // Note: table.getVersion may be null (pre-1.4.3 volumes)
            tableVersionMap.put(table.getTableId(), table.getVersion());
        }

        this.taskQueue = new DelayQueue<>();
        this.queueConsumer = new Thread(new QueueConsumer(this.taskQueue), "PSSM_QUEUE");
        this.queueConsumer.start();

        this.clearLatestCacheCallback = new Callback() {
            @Override
            public void run(Session session, long timestamp) {
                updateLatestAISCache(CACHE_SENTINEL);
            }
        };
        this.enqueueClearAndUpdateCallback = new Callback() {
            @Override
            public void run(Session session, long timestamp) {
                taskQueue.add(new UpdateLatestCacheTask(0));
                taskQueue.add(new ClearAISMapTask(0, 10000));
            }
        };

        this.aisCacheMissCount = new AtomicInteger(0);
        this.loadAISFromStorageCount = new AtomicInteger(0);
        this.delayedTreeCount = new AtomicInteger(0);

        if (!skipAISUpgrade) {
            // None
        }
        // else LOG.warn("Skipping AIS upgrade");

        registerSystemTables();
    }

    @Override
    public void stop() {
        stopConsumer();
        this.delayedTreeIDGenerator = null;
        this.aisMap = null;
        this.nameGenerator = null;
        this.latestAISCache = null;
        this.clearLatestCacheCallback = null;
        this.enqueueClearAndUpdateCallback = null;
        this.taskQueue.clear();
        this.taskQueue = null;
        this.queueConsumer = null;
        this.aisCacheMissCount = null;
        this.loadAISFromStorageCount = null;
        this.delayedTreeCount = null;
        CACHE_SENTINEL.sAIS.refCount.set(0);
        super.stop();
    }

    @Override
    public void crash() {
        stop();
    }

    private void stopConsumer() {
        final long endNanoTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        if(!taskQueue.offer(new StopTask(0))) {
            // DelayedQueue is unbounded, should never happen
            LOG.error("Could not offer StopTask");
        }
        for(;;) {
            long remaining = endNanoTime - System.nanoTime();
            if(remaining <= 0 || !queueConsumer.isAlive()) {
                break;
            }
            try {
                queueConsumer.join(100);
            } catch(InterruptedException e) {
                LOG.warn("Interrupted while trying to stop QueueConsumer");
            }
        }
    }

    private SharedAIS loadToShared(Session session, GenValue genValue, GenMap genMap) throws PersistitException {
        ProtobufReader reader = new ProtobufReader(getTypesRegistry(), storageFormatRegistry, new AkibanInformationSchema());
        loadFromStorage(session, reader);
        AkibanInformationSchema newAIS = finishReader(reader);
        return createValidatedShared(session, newAIS, genValue, genMap);
    }

    private void loadFromStorage(Session session, final ProtobufReader reader) throws PersistitException {
        treeService.visitStorage(session, new TreeVisitor() {
            @Override
            public void visit(Exchange ex) throws PersistitException {
                loadPrimaryProtobuf(ex, reader, null);
            }},
            SCHEMA_TREE_NAME
        );
    }

    private void loadProtobufChildren(Exchange ex, ProtobufReader reader, Collection<String> skipSchemas) throws PersistitException {
        ex.append(Key.BEFORE);
        while(ex.next()) {
            if((skipSchemas != null) && skipSchemas.contains(ex.getKey().indexTo(-1).decodeString())) {
                continue;
            }
            byte[] storedAIS = ex.getValue().getByteArray();
            ByteBuffer buffer = ByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }
    }

    private void loadPrimaryProtobuf(Exchange ex, ProtobufReader reader, Collection<String> skipSchemas) throws PersistitException {
        ex.clear().append(S_K_META_VERSION).fetch();
        if(!ex.getValue().isDefined()) {
            // Can only be empty if there is no data here
            ex.clear();
            if(ex.hasNext()) {
                throw new AkibanInternalException("Unsupported data volume (no metadata version key)");
            } else {
                // No data in this volume at all.
                return;
            }
        }
        long metaVersion = ex.getValue().getLong();
        if(CURRENT_META_VERSION != metaVersion) {
            throw new AkibanInternalException(String.format("Unsupported data volume meta version %d, current %d",
                                                            metaVersion, CURRENT_META_VERSION));
        }

        ex.clear().append(S_K_DATA_VERSION).fetch();
        assert ex.getValue().isDefined() : "No data version";
        long dataVersion = ex.getValue().getLong();
        if(CURRENT_DATA_VERSION != dataVersion) {
            throw new AkibanInternalException(String.format("Unsupported data volume data version %d, current %d",
                                                            dataVersion, CURRENT_DATA_VERSION));
        }

        ex.clear().append(S_K_PROTOBUF);
        loadProtobufChildren(ex, reader, skipSchemas);

        ex.clear().append(S_K_PROTOBUF_MEM).fetch();
        if(ex.getValue().isDefined()) {
            byte[] storedAIS = ex.getValue().getByteArray();
            ByteBuffer buffer = ByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }
    }

    private AkibanInformationSchema finishReader(ProtobufReader reader) {
        reader.loadAIS();
        for(Table table : reader.getAIS().getTables().values()) {
            // nameGenerator is only needed to generate hidden PK, which shouldn't happen here
            table.endTable(null);
        }
        return reader.getAIS();
    }

    private void buildRowDefs(Session session, AkibanInformationSchema newAis) throws PersistitException {
        treeService.getTableStatusCache().detachAIS();
        // TODO: this attaches the TableStatus to each table. 
        // This used to be done in RowDefBuilder#build() but no longer.
        for (final Table table : newAis.getTables().values()) {
            final TableStatus status;
            if (table.hasMemoryTableFactory()) {
                status = treeService.getTableStatusCache().getOrCreateMemoryTableStatus(table.getTableId(), MemoryAdapter.getMemoryTableFactory(table));
            } else {
                status = treeService.getTableStatusCache().createTableStatus(table);
            }
            table.tableStatus(status);
        }
        
        RowDefBuilder rowDefBuilder = new RowDefBuilder(null, newAis, treeService.getTableStatusCache());
        rowDefBuilder.build();
        // This creates|verifies the trees exist for sequences.
        // TODO: Why are sequences special here?
        sequenceTrees(newAis);
    }

    private void sequenceTrees (final AkibanInformationSchema newAis) throws PersistitException {
        for (Sequence sequence : newAis.getSequences().values()) {
            PersistitStorageDescription storageDescription = (PersistitStorageDescription)sequence.getStorageDescription();
            LOG.debug("registering sequence: {} with tree name: {}", sequence.getSequenceName(), storageDescription.getTreeName());
            // treeCache == null -> loading from start or creating a new sequence
            if (storageDescription.getTreeCache() == null) {
                treeService.populateTreeCache(storageDescription);
            }
        }
    }

    private void saveMetaAndDataVersions(Exchange ex) throws PersistitException {
        ex.clear().append(S_K_META_VERSION).getValue().put(CURRENT_META_VERSION);
        ex.store();
        ex.clear().append(S_K_DATA_VERSION).getValue().put(CURRENT_DATA_VERSION);
        ex.store();
    }

    private ByteBuffer storeProtobuf(Exchange ex,
                                     ByteBuffer buffer,
                                     AkibanInformationSchema newAIS,
                                     String schema) throws PersistitException {
        final ProtobufWriter.WriteSelector selector;
        switch(schema) {
            case TableName.INFORMATION_SCHEMA:
            case TableName.SECURITY_SCHEMA:
                selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                    @Override
                    public Columnar getSelected(Columnar columnar) {
                        if(columnar.isTable() && ((Table)columnar).hasMemoryTableFactory()) {
                            return null;
                        }
                        return columnar;
                    }
                };
            break;
            case TableName.SYS_SCHEMA:
            case TableName.SQLJ_SCHEMA:
                selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                    @Override
                    public boolean isSelected(Routine routine) {
                        return false;
                    }
                };
            break;
            default:
                selector = new ProtobufWriter.SingleSchemaSelector(schema);
        }

        if(newAIS.getSchema(schema) != null) {
            buffer = serialize(buffer, newAIS, selector);
            ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
            ex.store();
        } else {
            ex.remove();
        }

        return buffer;
    }

    private void saveMemoryTables(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        // Want *just* non-persisted memory tables and system routines
        final ProtobufWriter.WriteSelector selector = new ProtobufWriter.TableFilterSelector() {
            @Override
            public Columnar getSelected(Columnar columnar) {
                if(columnar.isAISTable() && ((Table)columnar).hasMemoryTableFactory()) {
                    return columnar;
                }
                return null;
            }

            @Override
            public boolean isSelected(Sequence sequence) {
                return false;
            }

            @Override
            public boolean isSelected(Routine routine) {
                return isSystemName(routine.getName());
            }

            @Override
            public boolean isSelected(SQLJJar sqljJar) {
                return isSystemName(sqljJar.getName());
            }

            protected boolean isSystemName(TableName name) {
                return TableName.SYS_SCHEMA.equals(name.getSchemaName()) ||
                       TableName.SQLJ_SCHEMA.equals(name.getSchemaName()) ||
                       TableName.SECURITY_SCHEMA.equals(name.getSchemaName());
            }
        };

        saveMetaAndDataVersions(ex);
        ByteBuffer buffer = serialize(null, newAIS, selector);
        ex.clear().append(S_K_PROTOBUF_MEM);
        ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
        ex.store();
    }

    private SharedAIS createValidatedShared(Session session,
                                            AkibanInformationSchema newAIS,
                                            GenValue genValue,
                                            GenMap genMap) {
        long generation = (genValue == GenValue.NEW) ? getNextGeneration(session) : getGenerationSnapshot(session);
        validateAndFreeze(newAIS, generation);

        // Constructed with ref count 0, attach bumps to 1
        final SharedAIS sAIS = new SharedAIS(newAIS);
        attachToSession(session, sAIS);

        if(genMap == GenMap.PUT_NEW) {
            saveNewAISInMap(sAIS);
        }
        return sAIS;
    }

    private static void validateAndFreeze(AkibanInformationSchema newAIS, long generation) {
        newAIS.validate(AISValidations.ALL_VALIDATIONS).throwIfNecessary();
        newAIS.setGeneration(generation);
        newAIS.freeze();
    }

    private void addCallbacksForAISChange(Session session) {
        txnService.addCallbackOnActive(session, CallbackType.PRE_COMMIT, clearLatestCacheCallback);
        txnService.addCallbackOnActive(session, CallbackType.END, enqueueClearAndUpdateCallback);
    }

    private void saveNewAISInMap(SharedAIS sAIS) {
        long generation = sAIS.ais.getGeneration();
        aisMap.putNewKey(generation, sAIS);
    }

    private boolean updateLatestAISCache(final AISAndTimestamp newCache) {
        // NB, see comment on variable and other usage before changing
        // As described in the comment, can't even consider updating cache while there is another outstanding
        // change. The count is 1 while held in the cache, so >1 means other outstanding changes.
        // Synchronized block so we can both change counter on sentinel and write to cache.
        synchronized(CACHE_SENTINEL) {
            if(latestAISCache == CACHE_SENTINEL) {
                if(newCache == CACHE_SENTINEL) {
                    CACHE_SENTINEL.sAIS.acquire();
                    return true;
                }
                int count = CACHE_SENTINEL.sAIS.release();
                if(count > 0) {
                    LOG.debug("Skipping cache update due to multiple outstanding changes: {}", count);
                    return false;
                }
                newCache.sAIS.acquire();
                latestAISCache = newCache;
                return true;
            } else {
                if(newCache != CACHE_SENTINEL) {
                    // Can happen if pre-commit hook doesn't get called (i.e. failure after SchemaManager call).
                    // In that case, the generation itself should not be changing -- just the timestamp.
                    if(latestAISCache.sAIS.ais.getGeneration() != newCache.sAIS.ais.getGeneration()) {
                        throw new IllegalStateException("Transition from non to non-sentinel for differing generations: "+
                                                        latestAISCache.toString() + " => " + newCache.toString());
                    }
                }
                latestAISCache.sAIS.release();
                newCache.sAIS.acquire();
                latestAISCache = newCache;
                return true;
            }
        }
    }

    @Override
    protected void storedAISChange(Session session,
                                   AkibanInformationSchema newAIS,
                                   Collection<String> schemaNames) {
        ByteBuffer buffer = null;
        Exchange ex = null;
        try {
            createValidatedShared(session, newAIS, GenValue.NEW, GenMap.NO_PUT);

            for(String schema : schemaNames) {
                ex = schemaTreeExchange(session);
                saveMetaAndDataVersions(ex);
                ex.clear().append(S_K_PROTOBUF).append(schema);
                buffer = storeProtobuf(ex, buffer, newAIS, schema);
                treeService.releaseExchange(session, ex);
                ex = null;
            }
            buildRowDefs(session, newAIS);
            addCallbacksForAISChange(session);
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
        }
    }

    private void serializeMemoryTables(Session session, AkibanInformationSchema newAIS) {
        Exchange ex = null;
        try {
            ex = schemaTreeExchange(session);
            saveMemoryTables(ex, newAIS);
            treeService.releaseExchange(session, ex);
            ex = null;
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
        }
    }

    @Override
    protected void unStoredAISChange(Session session, AkibanInformationSchema newAIS) {
        try {
            createValidatedShared(session, newAIS, GenValue.NEW, GenMap.NO_PUT);
            serializeMemoryTables(session, newAIS);
            buildRowDefs(session, newAIS);
            addCallbacksForAISChange(session);
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        }
    }

    @Override
    protected void storedOnlineChange(Session session,
                                      OnlineSession onlineSession,
                                      AkibanInformationSchema newAIS,
                                      Collection<String> schemas) {
        Exchange ex = schemaTreeExchange(session);
        try {
            // Get a unique generation for this AIS, but will only be visible to owning session
            createValidatedShared(session, newAIS, GenValue.NEW, GenMap.NO_PUT);

            // And a new generation for the current so no one else will think the one for newAIS is theirs
            getNextGeneration(session);

            saveMetaAndDataVersions(ex);

            // online,cid => generation
            ex.clear().append(S_K_ONLINE).append(onlineSession.id);
            ex.getValue().put(newAIS.getGeneration());
            ex.store();

            // online,cid,protobuf,schema = bytes
            ByteBuffer buffer = null;
            ex.append(S_K_PROTOBUF).append("");
            for(String name : schemas) {
                ex.to(name);
                buffer = storeProtobuf(ex, buffer, newAIS, name);
            }

            addCallbacksForAISChange(session);
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    protected void clearOnlineState(Session session, OnlineSession onlineSession) {
        Exchange ex = schemaTreeExchange(session);
        try {
            ex.clear().append(S_K_ONLINE).append(onlineSession.id);
            ex.remove(Key.GTEQ);
            for(int tid : onlineSession.tableIDs) {
                String treeName = onlineHandledHKeyTreeName(onlineSession.id, tid);
                if(treeService.treeExists(treeName)) {
                    Exchange changeEx = onlineHandledHKeyExchange(session, onlineSession.id, tid);
                    //
                    // remove() would be preferable but Persistit will block until transactions
                    // and journal can be fully pruned. Instead, clear and track for delayed.
                    //
                    changeEx.removeAll();
                    nameGenerator.generatedTreeName(treeName);
                    treeWasRemoved(session, SCHEMA_TREE_NAME, treeName);
                }
            }
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    protected OnlineCache buildOnlineCache(Session session) {
        Exchange ex = schemaTreeExchange(session);
        try{
            OnlineCache onlineCache = new OnlineCache();

            // Load affected schemas, then remaining schemas and create full AIS.
            ex.clear().append(S_K_ONLINE).append(Key.BEFORE);
            while(ex.next()) {
                long onlineID = ex.getKey().indexTo(-1).decodeLong();
                long generation = ex.getValue().getLong();

                int schemaCount = 0;
                ex.append(S_K_PROTOBUF).append(Key.BEFORE);
                while(ex.next()) {
                    ++schemaCount;
                    String schema = ex.getKey().indexTo(-1).decodeString();
                    Long prev = onlineCache.schemaToOnline.put(schema, onlineID);
                    assert (prev == null) : String.format("%s, %d, %d", schema, prev, onlineID);
                }

                ex.getKey().cut();
                if(generation != -1) {
                    ProtobufReader reader = new ProtobufReader(getTypesRegistry(), getStorageFormatRegistry());
                    loadProtobufChildren(ex, reader, null);
                    loadPrimaryProtobuf(ex, reader, onlineCache.schemaToOnline.keySet());

                    // Reader will have two copies of affected schemas, skip second (i.e. non-online)
                    AkibanInformationSchema newAIS = finishReader(reader);
                    validateAndFreeze(newAIS, generation);
                    buildRowDefs(session, newAIS);
                    onlineCache.onlineToAIS.put(onlineID, newAIS);
                } else if(schemaCount != 0) {
                    throw new IllegalStateException("No generation but had schemas");
                }

                ex.clear().append(S_K_ONLINE).append(onlineID).append(S_K_CHANGE).append(Key.BEFORE);
                while(ex.next()) {
                    int tid = ex.getKey().indexTo(-1).decodeInt();
                    Long prev = onlineCache.tableToOnline.put(tid, onlineID);
                    assert (prev == null) : String.format("%d, %d, %d", tid, prev, onlineID);
                    TableChanges.ChangeSet changeSet = ChangeSetHelper.load(ex.getValue().getByteArray());
                    onlineCache.onlineToChangeSets.put(onlineID, changeSet);
                }

                ex.clear().append(S_K_ONLINE).append(onlineID);
            }

            return onlineCache;
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    protected void newTableVersions(Session session, Map<Integer,Integer> newVersions) {
        // Schedule the update for the tableVersionMap version number on commit.
        Map<Integer,Integer> tableAndVersions = session.get(TABLE_VERSIONS);
        if(tableAndVersions == null) {
            tableAndVersions = new HashMap<>();
            session.put(TABLE_VERSIONS, tableAndVersions);
            txnService.addCallback(session, TransactionService.CallbackType.PRE_COMMIT, CLAIM_TABLE_VERSION_MAP);
            txnService.addCallback(session, TransactionService.CallbackType.COMMIT, BUMP_TABLE_VERSIONS);
            txnService.addCallback(session, TransactionService.CallbackType.END, CLEAR_TABLE_VERSIONS);
        }
        tableAndVersions.putAll(newVersions);
    }

    @Override
    protected void bumpGeneration(Session session) {
        getNextGeneration(session);
        addCallbacksForAISChange(session);
    }

    @Override
    protected long generateSaveOnlineSessionID(Session session) {
        Exchange ex = schemaTreeExchange(session);
        try {
            long id = ex.getTree().getSeqAccumulator(ACCUMULATOR_INDEX_ONLINE_ID).allocate();
            ex.clear().append(S_K_ONLINE).append(id);
            ex.getValue().put(-1L); // No generation yet
            ex.store();
            return id;
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    /** Serialize given AIS. Allocates a new buffer if necessary so always use <i>returned</i> buffer. */
    private static ByteBuffer serialize(ByteBuffer buffer, AkibanInformationSchema ais, WriteSelector selector) {
        ProtobufWriter writer = new ProtobufWriter(selector);
        writer.save(ais);
        int size = writer.getBufferSize();
        if(buffer == null || (buffer.capacity() < size)) {
            buffer = ByteBuffer.allocate(size);
        }
        buffer.clear();
        writer.serialize(buffer);
        buffer.flip();
        return buffer;
    }

    /** Public for test only. Should not generally be called. */
    public void cleanupDelayedTrees(final Session session, final boolean clearMemoryTables) throws PersistitException {
        treeService.visitStorage(
                session,
                new TreeVisitor() {
                    @Override
                    public void visit(final Exchange ex) throws PersistitException {
                        if(clearMemoryTables) {
                            // Don't reload memory table definitions
                            ex.clear().append(S_K_PROTOBUF_MEM).remove();
                        }
                        // Clear old trees
                        ex.clear();
                        KeyFilter filter = new KeyFilter().append(KeyFilter.simpleTerm(S_K_DELAYED));
                        while(ex.traverse(Key.Direction.GT, filter, Integer.MAX_VALUE)) {
                            ex.getKey().indexTo(1);     // skip delayed key
                            ex.getKey().decodeLong();   // skip id
                            long timestamp = ex.getKey().decodeLong();
                            ex.getValue().setStreamMode(true);
                            String schema = ex.getValue().getString();
                            String treeName = ex.getValue().getString();
                            ex.getValue().setStreamMode(false);
                            ex.remove();

                            LOG.debug("Removing delayed tree {} from timestamp {}", treeName, timestamp);
                            TreeLink link = treeService.treeLink(treeName);
                            Exchange ex2 = treeService.getExchange(session, link);
                            ex2.removeTree();
                            treeService.releaseExchange(session, ex2);

                            // Keep consistent if called during runtime
                            if(nameGenerator != null) {
                                nameGenerator.removeTreeName(treeName);
                            }
                        }
                    }
                },
                SCHEMA_TREE_NAME
        );
    }

    // Package for tests
    void clearAISMap() {
        aisMap.clear();
    }

    // Package for tests
    int clearUnreferencedAISMap() {
        aisMap.claimExclusive();
        try {
            int size = aisMap.size();
            if(size <= 1) {
                return size;
            }
            // Find newest generation
            Long maxGen = Long.MIN_VALUE;
            for(Long gen : aisMap.getWrappedMap().keySet()) {
                maxGen = Math.max(gen, maxGen);
            }
            // Remove all unreferenced (except the newest)
            Iterator<Map.Entry<Long,SharedAIS>> it = aisMap.getWrappedMap().entrySet().iterator();
            while(it.hasNext()) {
                Map.Entry<Long,SharedAIS> entry = it.next();
                if(!entry.getValue().isShared() && !entry.getKey().equals(maxGen)) {
                    it.remove();
                }
            }
            return aisMap.size();
        } finally {
            aisMap.releaseExclusive();
        }
    }

    // Package for Tests
    int getAISMapSize() {
        return aisMap.size();
    }

    // Package for tests
    public boolean waitForQueueToEmpty(long maxWaitMillis) {
        final int SHORT_WAIT_MILLIS = 5;
        final long endNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxWaitMillis);
        while(!taskQueue.isEmpty()) {
            final long remaining = endNanos - System.nanoTime();
            if(remaining < 0) {
                return false;
            }
            try {
                Thread.sleep(SHORT_WAIT_MILLIS, 0);
            } catch(InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return true;
    }

    private Exchange schemaTreeExchange(Session session) {
        TreeLink link = treeService.treeLink(SCHEMA_TREE_NAME);
        return treeService.getExchange(session, link);
    }

    private static String onlineHandledHKeyTreeName(long onlineID, int tableID) {
        return String.format(ONLINE_HANDLED_HKEY_TREE_FMT, onlineID, tableID);
    }

    private Exchange onlineHandledHKeyExchange(Session session, int tableID) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        if(onlineID == null) {
            throw new IllegalArgumentException("No online change for table: " + tableID);
        }
        return onlineHandledHKeyExchange(session, onlineID, tableID);
    }

    private Exchange onlineHandledHKeyExchange(Session session, long onlineID, int tableID) {
        TreeLink link = treeService.treeLink(onlineHandledHKeyTreeName(onlineID, tableID));
        return treeService.getExchange(session, link);
    }

    @Override
    public long getOldestActiveAISGeneration() {
        aisMap.claimShared();
        try {
            long min = Long.MAX_VALUE;
            for(Long l : aisMap.getWrappedMap().keySet()) {
                min = Math.min(min, l);
            }
            return min;
         } finally {
            aisMap.releaseShared();
        }
    }

    @Override
    public Set<Long> getActiveAISGenerations() {
        aisMap.claimShared();
        try {
            return new HashSet<>(aisMap.getWrappedMap().keySet());
         } finally {
            aisMap.releaseShared();
        }
    }

    private Accumulator.SeqAccumulator getGenerationAccumulator(Session session) throws PersistitException {
        Exchange ex = schemaTreeExchange(session);
        try {
            return ex.getTree().getSeqAccumulator(ACCUMULATOR_INDEX_SCHEMA_GEN);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    private long getGenerationSnapshot(Session session) {
        try {
            return getGenerationAccumulator(session).getSnapshotValue();
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private long getNextGeneration(Session session) {
        try {
            return getGenerationAccumulator(session).allocate();
        } catch(PersistitException | RollbackException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private void attachToSession(Session session, SharedAIS sAIS) {
        sAIS.acquire();
        SharedAIS old = session.put(SESSION_SAIS_KEY, sAIS);
        if(old != null) {
            old.release();
        } else {
            txnService.addCallbackOnActive(session, CallbackType.END, CLEAR_SESSION_KEY_CALLBACK);
        }
    }

    private class SchemaManagerSummaryFactory extends BasicFactoryBase {
        public SchemaManagerSummaryFactory() {
            super(new TableName(TableName.INFORMATION_SCHEMA, "schema_manager_summary"));
        }

        private class Scan implements MemoryGroupCursor.GroupScan {
            private final RowType rowType;
            private int pkCounter = 0;

            public Scan(RowType rowType) {
                this.rowType = rowType;
            }

            @Override
            public Row next() {
                if(pkCounter != 0) {
                    return null;
                }
                AISAndTimestamp latest = latestAISCache;
                return new ValuesHolderRow(rowType,
                                     aisCacheMissCount.get(),
                                     delayedTreeCount.get(),
                                     (latest == CACHE_SENTINEL) ? null : latest.sAIS.ais.getGeneration(),
                                     (latest == CACHE_SENTINEL) ? null : latest.timestamp,
                                     loadAISFromStorageCount.get(),
                                     aisMap.size(),
                                     CACHE_SENTINEL.sAIS.shareCount(),
                                     taskQueue.size(),
                                     ++ pkCounter
                );
            }

            @Override
            public void close() {
            }
        }

        @Override
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter, Group group) {
            return new Scan(getRowType(group.getAIS()));
        }

        @Override
        public long rowCount(Session sesion) {
            return 1;
        }
    }

    @Override
    protected void registerSystemTables() {
        super.registerSystemTables();
        registerSummaryTable();
    }

    private void registerSummaryTable() {
        SchemaManagerSummaryFactory factory = new SchemaManagerSummaryFactory();
        NewAISBuilder builder = AISBBasedBuilder.create(getTypesTranslator());
        builder.table(factory.getName())
                .colBigInt("cache_misses", false)
                .colBigInt("delayed_tree_count", false)
                .colBigInt("latest_generation", true)
                .colBigInt("latest_timestamp", true)
                .colBigInt("load_count", false)
                .colBigInt("map_size", false)
                .colBigInt("outstanding_count", false)
                .colBigInt("task_queue_size", false);

        AkibanInformationSchema ais = builder.ais();
        Table table = ais.getTable(factory.getName());
        registerMemoryInformationSchemaTable(table, factory);
    }

    @Override
    protected void clearTableStatus(Session session, Table table) {
        treeService.getTableStatusCache().clearTableStatus(session, table);
    }

    @Override
    protected void renamingTable(Session session, TableName oldName, TableName newName) {
        // None
    }

    private void updateTableVersionMap(Map<Integer,Integer> tableAndVersions) {
        for(Entry<Integer, Integer> entry : tableAndVersions.entrySet()) {
            int tableID = entry.getKey();
            int newVersion = entry.getValue();
            Integer current = tableVersionMap.get(entry.getKey());
            if(current != null && current >= newVersion) {
                throw new IllegalStateException("Current not < new: " + current + "," + newVersion);
            }
            boolean success = tableVersionMap.compareAndSet(tableID, current, newVersion);
            // Failed CAS would indicate concurrent DDL on this table, which should not be possible
            if(!success) {
                throw new IllegalStateException("Unexpected concurrent DDL on table: " + tableID);
            }
            LOG.trace("Bumped table {} version: {}", tableID, newVersion);
        }
    }

    private static final Callback CLEAR_SESSION_KEY_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            SharedAIS sAIS = session.remove(SESSION_SAIS_KEY);
            if(sAIS != null) {
                sAIS.release();
            }
        }
    };

    // If the Alter table fails, make sure to clean up the TABLE_VERSION change list on end
    // If the Alter succeeds, the bumpTableVersionCommit process will clean up, and this does nothing.
    protected final TransactionService.Callback CLEAR_TABLE_VERSIONS = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(TABLE_VERSIONS);
        }
    };

    protected final TransactionService.Callback BUMP_TABLE_VERSIONS = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            Map<Integer,Integer> tableAndVersions = session.remove(TABLE_VERSIONS);
            if(tableAndVersions != null) {
                updateTableVersionMap(tableAndVersions);
            }
        }
    };

    private final TransactionService.Callback CLAIM_TABLE_VERSION_MAP = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            tableVersionMap.claimExclusive();
            txnService.addCallback(session, CallbackType.END, RELEASE_TABLE_VERSION_MAP);
        }
    };

    private final TransactionService.Callback RELEASE_TABLE_VERSION_MAP = new Callback() {
        @Override
        public void run(Session session, long timestamp) {
            tableVersionMap.releaseExclusive();
        }
    };


    private abstract class QueueTask implements Delayed {
        protected final long initialDelay;
        protected final long rescheduleDelay;
        protected final long fireAt;

        protected QueueTask(long initialDelay) {
            this(initialDelay, -1);
        }

        protected QueueTask(long initialDelay, long rescheduleDelay) {
            this.initialDelay = initialDelay;
            this.rescheduleDelay = rescheduleDelay;
            this.fireAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(initialDelay);
        }

        @Override
        public String toString() {
            return getClass().getSimpleName();
        }

        @Override
        public final long getDelay(TimeUnit unit) {
            long diff = fireAt - System.nanoTime();
            return unit.convert(diff, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            if(this == o) {
                return 0;
            }
            long d = getDelay(TimeUnit.NANOSECONDS) - o.getDelay(TimeUnit.NANOSECONDS);
            return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
        }

        public boolean stopConsumer() {
            return false;
        }

        public boolean shouldReschedule() {
            return rescheduleDelay >= 0;
        }

        abstract public QueueTask cloneTask();
        abstract public boolean runTask() throws Exception;
    }

    private class StopTask extends QueueTask {
        public StopTask(long initialDelay) {
            super(initialDelay);
        }

        @Override
        public boolean stopConsumer() {
            return true;
        }

        @Override
        public StopTask cloneTask() {
            return new StopTask(rescheduleDelay);
        }

        @Override
        public boolean runTask() {
            return true;
        }
    }

    private class UpdateLatestCacheTask extends QueueTask {
        /** No reschedule as that would mess with the outstanding count **/
        protected UpdateLatestCacheTask(long initialDelay) {
            super(initialDelay);
        }

        @Override
        public UpdateLatestCacheTask cloneTask() {
            return new UpdateLatestCacheTask(initialDelay);
        }

        @Override
        public boolean runTask() throws PersistitException {
            try (Session session = sessionService.createSession()) {
                doCacheUpdate(session);
                return true;
            }
        }

        private void doCacheUpdate(Session session) throws PersistitException {
            txnService.beginTransaction(session);
            try {
                // AIS from DDL is not put into the aisMap so if no one has read it yet, this sill cause a
                // reload from disk. No matter where it comes from, always OK to try and update cache.
                final SharedAIS sAIS = PersistitStoreSchemaManager.this.getSharedAIS(session);
                // Attempt to update cache with our start timestamp, because that is what our snapshot is valid for.
                final long startTime = txnService.getTransactionStartTimestamp(session);
                updateLatestAISCache(new AISAndTimestamp(sAIS, startTime));
                txnService.commitTransaction(session);
            } finally {
                txnService.rollbackTransactionIfOpen(session);
            }
        }
    }

    private class ClearAISMapTask extends QueueTask {
        protected ClearAISMapTask(long initialDelay, long rescheduleDelay) {
            super(initialDelay, rescheduleDelay);
        }

        @Override
        public ClearAISMapTask cloneTask() {
            return new ClearAISMapTask(rescheduleDelay, rescheduleDelay);
        }

        @Override
        public boolean runTask() {
            int remaining = PersistitStoreSchemaManager.this.clearUnreferencedAISMap();
            return (remaining <= 1); // Success <= 1 entries in aisMap
        }
    }

    public static class QueueConsumer implements Runnable {
        private final BlockingQueue<QueueTask> queue;

        public QueueConsumer(BlockingQueue<QueueTask> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            boolean running = true;
            while(running) {
                QueueTask task = null;
                try {
                    task = queue.take();
                    LOG.trace("Running task {}", task);
                    if(task.runTask()) {
                        running = !task.stopConsumer();
                    } else {
                        if(task.shouldReschedule()) {
                            LOG.trace("Rescheduling task {}", task);
                            QueueTask newTask = task.cloneTask();
                            queue.add(newTask);
                        }
                    }
                } catch(InterruptedException e) {
                    running = false;
                } catch(RuntimeException e) {
                    LOG.error("RuntimeException" + fromTask(task), e);
                } catch(Exception e) {
                    LOG.error("Exception" + fromTask(task), e);
                } catch(Error e) {
                    LOG.error("Error (aborting consuming)", e);
                    throw e;
                }
            }
            LOG.trace("Exiting consumer");
        }

        private static String fromTask(QueueTask task) {
            return (task != null) ? " from task " + task :  "";
        }
    }
}
