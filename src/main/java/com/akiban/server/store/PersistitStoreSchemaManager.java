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

package com.akiban.server.store;

import com.akiban.ais.AISCloner;
import com.akiban.ais.model.AISBuilder;
import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Columnar;
import com.akiban.ais.model.DefaultNameGenerator;
import com.akiban.ais.model.NameGenerator;
import com.akiban.ais.model.Routine;
import com.akiban.ais.model.SQLJJar;
import com.akiban.ais.model.Sequence;
import com.akiban.ais.model.SynchronizedNameGenerator;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.ais.model.validation.AISValidations;
import com.akiban.ais.protobuf.ProtobufReader;
import com.akiban.ais.protobuf.ProtobufWriter;
import com.akiban.ais.util.UuidAssigner;
import com.akiban.qp.memoryadapter.BasicFactoryBase;
import com.akiban.qp.memoryadapter.MemoryAdapter;
import com.akiban.qp.memoryadapter.MemoryGroupCursor;
import com.akiban.qp.memoryadapter.MemoryTableFactory;
import com.akiban.qp.row.Row;
import com.akiban.qp.row.ValuesRow;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.PersistitAccumulatorTableStatusCache;
import com.akiban.server.error.AISTooLargeException;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedMetadataTypeException;
import com.akiban.server.error.UnsupportedMetadataVersionException;
import com.akiban.server.rowdata.RowDefCache;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.security.SecurityService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.service.tree.TreeVisitor;
import com.akiban.server.util.ReadWriteMap;
import com.akiban.util.GrowableByteBuffer;
import com.google.inject.Inject;
import com.persistit.Accumulator;
import com.persistit.Exchange;
import com.persistit.Key;
import com.persistit.KeyFilter;
import com.persistit.exception.PersistitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.BufferOverflowException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.akiban.qp.persistitadapter.PersistitAdapter.wrapPersistitException;
import static com.akiban.server.service.transaction.TransactionService.Callback;
import static com.akiban.server.service.transaction.TransactionService.CallbackType;
import static com.akiban.server.service.tree.TreeService.SCHEMA_TREE_NAME;

/**
 * <p>
 * Storage as of v1.2.1 (05/2012), Protobuf bytes per schema:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema1"</td>
 *         <td>byte[]</td>
 *         <td>Key is a pair of version (int, see {@link #PROTOBUF_PSSM_VERSION} for current) and schema name (string).<br>
 *             Value is as constructed by {@link ProtobufWriter}.
 *     </tr>
 *     <tr>
 *         <td>"byPBAIS",(int)version,"schema2"</td>
 *         <td>...</td>
 *         <td>...</td>
 *     </tr>
 * </table>
 * <br>
 * <p>
 * Storage as of v0.4 (05/2011), MetaModel bytes per AIS:
 * <table border="1">
 *     <tr>
 *         <th>key</th>
 *         <th>value</th>
 *         <th>description</th>
 *     </tr>
 *     <tr>
 *         <td>"byAIS"</td>
 *         <td>byte[]</td>
 *         <td>Value is as constructed by (now deleted) com.akiban.ais.metamodel.io.MessageTarget</td>
 *     </tr>
 * </table>
 * </p>
 * </p>
 */
public class PersistitStoreSchemaManager extends AbstractSchemaManager implements Service {
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

    private static final String AIS_KEY_PREFIX = "by";
    private static final String AIS_PROTOBUF_PARENT_KEY = AIS_KEY_PREFIX + "PBAIS";
    private static final String AIS_MEMORY_TABLE_KEY = AIS_KEY_PREFIX + "PBMEMAIS";
    private static final String DELAYED_TREE_KEY = "delayedTree";

    private static final int SCHEMA_GEN_ACCUM_INDEX = 0;

    // Changed from 1 to 2 due to incompatibility related to index row changes (see bug 985007)
    private static final int PROTOBUF_PSSM_VERSION = 2;

    private static final Session.Key<SharedAIS> SESSION_SAIS_KEY = Session.Key.named("SAIS_KEY");

    /**
     * <p>This is used as unusable cache identifier, count for outstanding DDLs, and sync object for updating
     * the latest cache value.</p>
     * <p>Why are all needed? Consider the simple case two concurrent DDLs, A and B. Both are have pre-committed
     * and cleared the cache. If A commits, the cache is updated, and then B commits, any reader using the cache
     * based on timestamp alone will get a stale snapshot. So, the cache can *only* be updated when there are no
     * more outstanding.</p>
     */
    private static final AISAndTimestamp CACHE_SENTINEL = new AISAndTimestamp(new SharedAIS(null), Long.MAX_VALUE);

    private static final Logger LOG = LoggerFactory.getLogger(PersistitStoreSchemaManager.class.getName());

    private final TreeService treeService;
    private final TransactionService txnService;
    private RowDefCache rowDefCache;
    private NameGenerator nameGenerator;
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

    @Inject
    public PersistitStoreSchemaManager(ConfigurationService config, SessionService sessionService,
                                       TreeService treeService, TransactionService txnService) {
        super(config, sessionService);
        this.treeService = treeService;
        this.txnService = txnService;
    }

    @Override
    protected NameGenerator getNameGenerator() {
        return nameGenerator;
    }

    @Override
    public void setSecurityService(SecurityService securityService) {
        this.securityService = securityService;
    }

    @Override
    public AkibanInformationSchema getAis(Session session) {
        return getAISInternal(session).ais;
    }

    private SharedAIS getAISInternal(Session session) {
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
                        local = loadAISFromStorage(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);
                        buildRowDefCache(session, local.ais);
                    } catch(PersistitException e) {
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

    @Override
    public boolean treeRemovalIsDelayed() {
        return true;
    }

    @Override
    public void treeWasRemoved(Session session, String schema, String treeName) {
        if(!treeRemovalIsDelayed()) {
            nameGenerator.removeTreeName(treeName);
            return;
        }

        LOG.debug("Delaying removal of tree (until next restart): {}", treeName);
        delayedTreeCount.incrementAndGet();
        Exchange ex = schemaTreeExchange(session, schema);
        try {
            long id = delayedTreeIDGenerator.incrementAndGet();
            long timestamp = System.currentTimeMillis();
            ex.clear().append(DELAYED_TREE_KEY).append(id).append(timestamp);
            ex.getValue().setStreamMode(true);
            ex.getValue().put(schema);
            ex.getValue().put(treeName);
            ex.getValue().setStreamMode(false);
            ex.store();
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    @Override
    public void start() {
        super.start();

        rowDefCache = new RowDefCache(treeService.getTableStatusCache());
        boolean skipAISUpgrade = Boolean.parseBoolean(config.getProperty(SKIP_AIS_UPGRADE_PROPERTY));

        this.aisMap = ReadWriteMap.wrapNonFair(new HashMap<Long,SharedAIS>());

        final int[] ordinalChangeCount = { 0 };
        AkibanInformationSchema newAIS = transactionally(
                sessionService.createSession(),
                new ThrowingCallable<AkibanInformationSchema>() {
                    @Override
                    public AkibanInformationSchema runAndReturn(Session session) throws PersistitException {
                        // Unrelated to loading, but fine time to do it
                        cleanupDelayedTrees(session);

                        SharedAIS sAIS = loadAISFromStorage(session, GenValue.SNAPSHOT, GenMap.PUT_NEW);

                        // Migrate requires RowDefs, but shouldn't generate ordinals (otherwise nulls will be lost)
                        buildRowDefCache(session, sAIS.ais, true);
                        ordinalChangeCount[0] = migrateAccumulatorOrdinals(sAIS.ais);
                        // And generate it fully now that ordinals are definitely set
                        buildRowDefCache(session, sAIS.ais, false);

                        long startTimestamp = txnService.getTransactionStartTimestamp(session);
                        sAIS.acquire(); // So count while in cache is 1
                        latestAISCache = new AISAndTimestamp(sAIS, startTimestamp);
                        return sAIS.ais;
                    }
                }
        );

        this.nameGenerator = SynchronizedNameGenerator.wrap(new DefaultNameGenerator(newAIS));
        this.delayedTreeIDGenerator = new AtomicLong();
        this.tableVersionMap = ReadWriteMap.wrapNonFair(new HashMap<Integer,Integer>());
        for(UserTable table : newAIS.getUserTables().values()) {
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
            final AkibanInformationSchema upgradeAIS = AISCloner.clone(newAIS);
            UuidAssigner uuidAssigner = new UuidAssigner();
            upgradeAIS.traversePostOrder(uuidAssigner);
            if(uuidAssigner.assignedAny() || ordinalChangeCount[0] > 0) {
                transactionally(sessionService.createSession(), new ThrowingCallable<Void>() {
                    @Override
                    public Void runAndReturn(Session session) throws PersistitException {
                        saveAISChangeWithRowDefs(session, upgradeAIS, upgradeAIS.getSchemas().keySet());
                        return null;
                    }
                });
            }
        }
        // else LOG.warn("Skipping AIS upgrade");

        registerSummaryTable();
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

    private SharedAIS loadAISFromStorage(final Session session, GenValue genValue, GenMap genMap) throws PersistitException {
        final AkibanInformationSchema newAIS = new AkibanInformationSchema();
        treeService.visitStorage(
                session,
                new TreeVisitor() {
                    @Override
                    public void visit(Exchange ex) throws PersistitException{
                        SerializationType typeForVolume = detectSerializationType(session, ex);
                        switch(typeForVolume) {
                            case NONE:
                                // Empty tree, nothing to do
                            break;
                            case PROTOBUF:
                                checkAndSetSerialization(typeForVolume);
                                loadProtobuf(ex, newAIS);
                            break;
                            default:
                                throw new UnsupportedMetadataTypeException(typeForVolume.name());
                        }
                    }
                },
                SCHEMA_TREE_NAME
        );
        for(Map.Entry<TableName,MemoryTableFactory> entry : memoryTableFactories.entrySet()) {
            UserTable table = newAIS.getUserTable(entry.getKey());
            if(table != null) {
                table.setMemoryTableFactory(entry.getValue());
            }
        }
        return validateAndFreeze(session, newAIS, genValue, genMap);
    }

    private static void loadProtobuf(Exchange ex, AkibanInformationSchema newAIS) throws PersistitException {
        ProtobufReader reader = new ProtobufReader(newAIS);
        Key key = ex.getKey();
        ex.clear().append(AIS_KEY_PREFIX);
        KeyFilter filter = new KeyFilter().append(KeyFilter.simpleTerm(AIS_PROTOBUF_PARENT_KEY));
        while(ex.traverse(Key.Direction.GT, filter, Integer.MAX_VALUE)) {
            if(key.getDepth() != 3) {
                throw new IllegalStateException("Unexpected " + AIS_PROTOBUF_PARENT_KEY + " format: " + key);
            }

            key.indexTo(1);
            int storedVersion = key.decodeInt();
            String storedSchema = key.decodeString();
            if(storedVersion != PROTOBUF_PSSM_VERSION) {
                LOG.debug("Unsupported version {} for schema {}", storedVersion, storedSchema);
                throw new UnsupportedMetadataVersionException(PROTOBUF_PSSM_VERSION, storedVersion);
            }

            byte[] storedAIS = ex.getValue().getByteArray();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }

        ex.clear().append(AIS_MEMORY_TABLE_KEY).fetch();
        if(ex.getValue().isDefined()) {
            byte[] storedAIS = ex.getValue().getByteArray();
            GrowableByteBuffer buffer = GrowableByteBuffer.wrap(storedAIS);
            reader.loadBuffer(buffer);
        }

        reader.loadAIS();

        // ProtobufWriter does not save group tables (by design) so generate columns and indexes
        AISBuilder builder = new AISBuilder(newAIS);
        builder.groupingIsComplete();
    }

    private void buildRowDefCache(Session session, AkibanInformationSchema newAis ) throws PersistitException {
        buildRowDefCache(session, newAis, false);
    }

    private void buildRowDefCache(Session session, AkibanInformationSchema newAis, boolean skipOrdinals) throws PersistitException {
        treeService.getTableStatusCache().detachAIS();
        // This create|verifies the trees exist for indexes & tables
        if(skipOrdinals) {
            rowDefCache.setAISWithoutOrdinals(session, newAis);
        } else {
            rowDefCache.setAIS(session, newAis);
        }
        // This creates|verifies the trees exist for sequences.
        // TODO: Why are sequences special here?
        sequenceTrees(newAis);
    }

    private void sequenceTrees (final AkibanInformationSchema newAis) throws PersistitException {
        for (Sequence sequence : newAis.getSequences().values()) {
            LOG.debug("registering sequence: " + sequence.getSequenceName() + " with tree name: " + sequence.getTreeName());
            // treeCache == null -> loading from start or creating a new sequence
            if (sequence.getTreeCache() == null) {
                treeService.populateTreeCache(sequence);
            }
        }
    }

    private void saveProtobuf(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, final String schema)
            throws PersistitException {
        final ProtobufWriter.WriteSelector selector;
        if(TableName.INFORMATION_SCHEMA.equals(schema) ||
           TableName.SECURITY_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                @Override
                public Columnar getSelected(Columnar columnar) {
                    if(columnar.isTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
                        return null;
                    }
                    return columnar;
                }
            };
        } else if(TableName.SYS_SCHEMA.equals(schema) ||
                  TableName.SQLJ_SCHEMA.equals(schema)) {
            selector = new ProtobufWriter.SingleSchemaSelector(schema) {
                    @Override
                    public boolean isSelected(Routine routine) {
                        return false;
                    }
            };
        } else {
            selector = new ProtobufWriter.SingleSchemaSelector(schema);
        }

        ex.clear().append(AIS_PROTOBUF_PARENT_KEY).append(PROTOBUF_PSSM_VERSION).append(schema);
        if(newAIS.getSchema(schema) != null) {
            buffer.clear();
            new ProtobufWriter(buffer, selector).save(newAIS);
            buffer.flip();

            ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
            ex.store();
        } else {
            ex.remove();
        }
    }

    private void saveMemoryTables(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS) throws PersistitException {
        // Want *just* non-persisted memory tables and system routines
        final ProtobufWriter.WriteSelector selector = new ProtobufWriter.TableFilterSelector() {
            @Override
            public Columnar getSelected(Columnar columnar) {
                if(columnar.isAISTable() && ((UserTable)columnar).hasMemoryTableFactory()) {
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
                return TableName.SYS_SCHEMA.equals(routine.getName().getSchemaName()) ||
                       TableName.SQLJ_SCHEMA.equals(routine.getName().getSchemaName()) ||
                       TableName.SECURITY_SCHEMA.equals(routine.getName().getSchemaName());
            }

            @Override
            public boolean isSelected(SQLJJar sqljJar) {
                return false;
            }
        };

        buffer.clear();
        new ProtobufWriter(buffer, selector).save(newAIS);
        buffer.flip();
        ex.clear().append(AIS_MEMORY_TABLE_KEY);
        ex.getValue().clear().putByteArray(buffer.array(), buffer.position(), buffer.limit());
        ex.store();
    }

    private SharedAIS validateAndFreeze(Session session, AkibanInformationSchema newAIS, GenValue genValue, GenMap genMap) {
        newAIS.validate(AISValidations.LIVE_AIS_VALIDATIONS).throwIfNecessary(); // TODO: Often redundant, cleanup
        long generation = (genValue == GenValue.NEW) ? getNextGeneration(session) : getGenerationSnapshot(session);
        newAIS.setGeneration(generation);
        newAIS.freeze();

        // Constructed with ref count 0, attach bumps to 1
        final SharedAIS sAIS = new SharedAIS(newAIS);
        attachToSession(session, sAIS);

        if(genMap == GenMap.PUT_NEW) {
            saveNewAISInMap(sAIS);
        }
        return sAIS;
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

    private GrowableByteBuffer newByteBufferForSavingAIS() {
        int maxSize = maxAISBufferSize == 0 ? Integer.MAX_VALUE : maxAISBufferSize;
        return new GrowableByteBuffer(4096, 4096, maxSize);
    }

    @Override
    protected void saveAISChangeWithRowDefs(Session session,
                                            AkibanInformationSchema newAIS,
                                            Collection<String> schemaNames) {
        GrowableByteBuffer byteBuffer = newByteBufferForSavingAIS();
        Exchange ex = null;
        try {
            validateAndFreeze(session, newAIS, GenValue.NEW, GenMap.NO_PUT);
            for(String schema : schemaNames) {
                ex = schemaTreeExchange(session, schema);
                checkAndSerialize(ex, byteBuffer, newAIS, schema);
                treeService.releaseExchange(session, ex);
                ex = null;
            }
            buildRowDefCache(session, newAIS);
            addCallbacksForAISChange(session);
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(byteBuffer.getMaxBurstSize());
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
        }
    }

    @Override
    protected void serializeMemoryTables(Session session, AkibanInformationSchema newAIS) {
        GrowableByteBuffer byteBuffer = newByteBufferForSavingAIS();
        Exchange ex = null;
        try {
            ex = schemaTreeExchange(session, TableName.INFORMATION_SCHEMA);
            saveMemoryTables(ex, byteBuffer, newAIS);
            treeService.releaseExchange(session, ex);
            ex = null;
        } catch(BufferOverflowException e) {
            throw new AISTooLargeException(byteBuffer.getMaxBurstSize());
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } finally {
            if(ex != null) {
                treeService.releaseExchange(session, ex);
            }
        }
    }

    @Override
    protected void unSavedAISChangeWithRowDefs(Session session, AkibanInformationSchema newAIS) {
        try {
            validateAndFreeze(session, newAIS, GenValue.NEW, GenMap.NO_PUT);
            serializeMemoryTables(session, newAIS);
            buildRowDefCache(session, newAIS);
            addCallbacksForAISChange(session);
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    public static SerializationType detectSerializationType(Session session, Exchange ex) {
        try {
            SerializationType type = SerializationType.NONE;

            // Simple heuristic to determine which style AIS storage we have
            boolean hasProtobuf = false;
            boolean hasUnknown = false;

            ex.clear().append(AIS_KEY_PREFIX);
            while(ex.next(true)) {
                if(ex.getKey().decodeType() != String.class) {
                    break;
                }
                String k = ex.getKey().decodeString();
                if(!k.startsWith(AIS_KEY_PREFIX)) {
                    break;
                }
                if(k.equals(AIS_PROTOBUF_PARENT_KEY) || k.equals(AIS_MEMORY_TABLE_KEY)) {
                    hasProtobuf = true;
                } else {
                    hasUnknown = true;
                }
            }

            if(hasUnknown && hasProtobuf) {
                throw new IllegalStateException("Both multiple serializations");
            } else if(hasProtobuf) {
                type = SerializationType.PROTOBUF;
            } else if(hasUnknown) {
                type = SerializationType.UNKNOWN;
            }

            return type;
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private void checkAndSetSerialization(SerializationType newSerializationType) {
        if((serializationType != SerializationType.NONE) && (serializationType != newSerializationType)) {
            throw new IllegalStateException("Mixed serialization types: " + serializationType + " vs " + newSerializationType);
        }
        serializationType = newSerializationType;
    }

    /** Public for test only. Should not generally be called. */
    public void cleanupDelayedTrees(final Session session) throws PersistitException {
        treeService.visitStorage(
                session,
                new TreeVisitor() {
                    @Override
                    public void visit(final Exchange ex) throws PersistitException {
                        // Don't reload memory table definitions
                        ex.clear().append(AIS_MEMORY_TABLE_KEY).remove();
                        // Clear old trees
                        ex.clear().append(DELAYED_TREE_KEY);
                        KeyFilter filter = new KeyFilter().append(KeyFilter.simpleTerm(DELAYED_TREE_KEY));
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
                            TreeLink link = treeService.treeLink(schema, treeName);
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

    private Exchange schemaTreeExchange(Session session, String schema) {
        TreeLink link = treeService.treeLink(schema, SCHEMA_TREE_NAME);
        return treeService.getExchange(session, link);
    }

    /**
     * @return Current serialization type
     */
    public SerializationType getSerializationType() {
        return serializationType;
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
    public boolean hasTableChanged(Session session, int tableID) {
        UserTable table = getAis(session).getUserTable(tableID);
        if(table == null) {
            throw new IllegalStateException("Unknown table: " + tableID);
        }
        Integer curVer = tableVersionMap.get(tableID);
        Integer tableVer = table.getVersion();
        if(curVer == null) {
            return tableVer != null;
        }
        return !curVer.equals(tableVer);
    }

    private Accumulator.SeqAccumulator getGenerationAccumulator(Session session) throws PersistitException {
        // treespace policy could split the _schema_ tree across volumes and give us multiple accumulators, which would
        // be very bad. Work around that with a fake/constant schema name. It isn't a problem if this somehow got changed
        // across a restart. Really, we want a constant, system-like volume to put this in.
        final String SCHEMA = "pssm";
        Exchange ex = schemaTreeExchange(session, SCHEMA);
        try {
            return ex.getTree().getSeqAccumulator(SCHEMA_GEN_ACCUM_INDEX);
        } finally {
            treeService.releaseExchange(session, ex);
        }
    }

    private long getGenerationSnapshot(Session session) {
        try {
            return getGenerationAccumulator(session).getSnapshotValue();
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        }
    }

    private long getNextGeneration(Session session) {
        try {
            return getGenerationAccumulator(session).allocate();
        } catch(PersistitException e) {
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

    private void checkAndSerialize(Exchange ex, GrowableByteBuffer buffer, AkibanInformationSchema newAIS, String schema) throws PersistitException {
        if(serializationType == SerializationType.NONE) {
            serializationType = DEFAULT_SERIALIZATION;
        }
        if(serializationType == SerializationType.PROTOBUF) {
            saveProtobuf(ex, buffer, newAIS, schema);
        } else {
            throw new IllegalStateException("Cannot serialize as " + serializationType);
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
                return new ValuesRow(rowType,
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
        public MemoryGroupCursor.GroupScan getGroupScan(MemoryAdapter adapter) {
            return new Scan(getRowType(adapter));
        }

        @Override
        public long rowCount() {
            return 1;
        }
    }

    private void registerSummaryTable() {
        SchemaManagerSummaryFactory factory = new SchemaManagerSummaryFactory();
        NewAISBuilder builder = AISBBasedBuilder.create();
        builder.userTable(factory.getName())
                .colBigInt("cache_misses", false)
                .colBigInt("delayed_tree_count", false)
                .colBigInt("latest_generation", true)
                .colBigInt("latest_timestamp", true)
                .colBigInt("load_count", false)
                .colBigInt("map_size", false)
                .colBigInt("outstanding_count", false)
                .colBigInt("task_queue_size", false);

        AkibanInformationSchema ais = builder.ais();
        UserTable table = ais.getUserTable(factory.getName());
        registerMemoryInformationSchemaTable(table, factory);
    }

    @Override
    protected <V> V transactionally(Session session, ThrowingCallable<V> callable) {
        txnService.beginTransaction(session);
        try {
            V ret = callable.runAndReturn(session);
            txnService.commitTransaction(session);
            return ret;
        } catch(PersistitException e) {
            throw wrapPersistitException(session, e);
        } catch(RuntimeException e) {
            throw e;
        } catch(Exception e) {
            throw new AkibanInternalException("Unexpected", e);
        } finally {
            txnService.rollbackTransactionIfOpen(session);
            session.close();
        }
    }

    @Override
    protected void deleteTableStatuses(Session session, Collection<Integer> tableIDs) {
        // Nothing extra to do, table status state stored in accumulators, which are attached to the trees.
    }

    /**
     * Find UserTables without ordinals and look them up in the old Accumulator based location.
     * @return count of tables whose ordinal was updated
     */
    private int migrateAccumulatorOrdinals(AkibanInformationSchema newAIS) {
        if(!(treeService.getTableStatusCache() instanceof PersistitAccumulatorTableStatusCache)) {
            return 0;
        }
        PersistitAccumulatorTableStatusCache tsc = (PersistitAccumulatorTableStatusCache)treeService.getTableStatusCache();
        int recoveredCount = 0;
        for(UserTable table : newAIS.getUserTables().values()) {
            if(!table.hasMemoryTableFactory() && (table.getOrdinal() == null)) {
                int ordinal = tsc.recoverAccumulatorOrdinal(table.rowDef().getTableStatus());
                table.setOrdinal(ordinal);
                ++recoveredCount;
            }
        }
        if(recoveredCount > 0) {
            LOG.info("Migrated {} ordinal values", recoveredCount);
        }
        return recoveredCount;
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
                final SharedAIS sAIS = PersistitStoreSchemaManager.this.getAISInternal(session);
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
