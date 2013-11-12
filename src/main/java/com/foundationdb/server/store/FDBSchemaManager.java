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
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.SynchronizedNameGenerator;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import com.foundationdb.async.Function;
import com.foundationdb.server.FDBTableStatusCache;
import com.foundationdb.server.collation.AkCollatorFactory;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.RowDefCache;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.ServiceManager;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.store.FDBTransactionService.TransactionState;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.FDBStorageFormatRegistry;
import com.foundationdb.KeyValue;
import com.foundationdb.Range;
import com.foundationdb.Transaction;
import com.foundationdb.tuple.Tuple;
import com.foundationdb.util.layers.DirectorySubspace;
import com.foundationdb.util.layers.Subspace;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   schemaManager/
 *     online/
 *       id/
 *         protobuf/
 *           schema_name    => byte[] (AIS Protobuf)
 *         changes/
 *           tid            => byte[] (ChangeSet Protobuf)
 *         generation       => long   (session's generation)
 *     protobuf/
 *       schema_name        => byte[] (AIS Protobuf)
 *     generation           => long
 *     dataVersion          => long
 *     metaDataVersion      => long
 *     onlineSession        => long
 * </pre>
 *
 * Transactional Reasoning:
 * <ul>
 *     <li>All consumers of getAis() do a full read of the generation key to determine the proper version.</li>
 *     <li>All DDL executors increment the generation while making the AIS changes</li>
 *     <li>Whenever a new AIS is read, the name generator and table version map is re-set</li>
 *     <li>Since there can be exactly one change to the generation at a time, all generated names and ids will be unique</li>
 * </ul>
 */
public class FDBSchemaManager extends AbstractSchemaManager implements Service, TableListener
{
    private static final Logger LOG = LoggerFactory.getLogger(FDBSchemaManager.class);

    private static final Tuple SCHEMA_MANAGER_TUPLE = Tuple.from("schemaManager");
    private static final Tuple PROTOBUF_TUPLE = Tuple.from("protobuf");
    private static final Tuple ONLINE_TUPLE = Tuple.from("online");
    private static final Tuple CHANGES_TUPLE = Tuple.from("changes");
    private static final String GENERATION_KEY = "generation";
    private static final String DATA_VERSION_KEY = "dataVersion";
    private static final String META_VERSION_KEY = "metaDataVersion";
    private static final String ONLINE_SESSION_KEY = "onlineSession";

    /** 1) Initial */
    private static final long CURRENT_DATA_VERSION = 1;
    /** 1) Initial directory based  2) Online metadata support */
    private static final long CURRENT_META_VERSION = 2;

    private static final Session.Key<AkibanInformationSchema> SESSION_AIS_KEY = Session.Key.named("AIS_KEY");
    private static final AkibanInformationSchema SENTINEL_AIS = new AkibanInformationSchema(Integer.MIN_VALUE);


    private final FDBHolder holder;
    private final FDBTransactionService txnService;
    private final ListenerService listenerService;
    private final ServiceManager serviceManager;
    private final Object AIS_LOCK = new Object();

    private DirectorySubspace rootDir;
    private DirectorySubspace smDirectory;
    private byte[] packedGenKey;
    private FDBTableStatusCache tableStatusCache;
    private RowDefCache rowDefCache;
    private AkibanInformationSchema curAIS;
    private NameGenerator nameGenerator;
    private AkibanInformationSchema memoryTableAIS;


    @Inject
    public FDBSchemaManager(ConfigurationService config,
                            SessionService sessionService,
                            FDBHolder holder,
                            TransactionService txnService,
                            ListenerService listenerService,
                            ServiceManager serviceManager) {
        super(config, sessionService, txnService, new FDBStorageFormatRegistry());
        this.holder = holder;
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("May only be ran with FDBTransactionService");
        }
        this.listenerService = listenerService;
        this.serviceManager = serviceManager;
    }


    //
    // Service
    //

    @Override
    public void start() {
        super.start();
        AkCollatorFactory.setUseKeyCoder(false);

        rootDir = holder.getRootDirectory();
        smDirectory = holder.getDatabase().run(new Function<Transaction, DirectorySubspace>()
        {
            @Override
            public DirectorySubspace apply(Transaction tr) {
                return holder.getRootDirectory().createOrOpen(tr, SCHEMA_MANAGER_TUPLE);
            }
        });

        // Cache as it used for every AIS access,
        packedGenKey = smDirectory.pack(GENERATION_KEY);

        this.memoryTableAIS = new AkibanInformationSchema();
        this.tableStatusCache = new FDBTableStatusCache(holder, txnService);
        this.rowDefCache = new RowDefCache(tableStatusCache);

        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    TransactionState txn = txnService.getTransaction(session);
                    // Check version compatibility (no upgrades currently) and save current if none present
                    boolean dataPresent = checkDataVersions(txn);
                    if(!dataPresent) {
                        saveMetaAndDataVersions(txn);
                    }
                    // And load checks the versions on every call (i.e. every generation change)
                    AkibanInformationSchema newAIS = loadFromStorage(session);
                    buildRowDefCache(session, newAIS);
                    FDBSchemaManager.this.curAIS = newAIS;
                }
            });
        }

        this.nameGenerator = SynchronizedNameGenerator.wrap(new DefaultNameGenerator(curAIS));
        mergeNewAIS(curAIS);

        listenerService.registerTableListener(this);
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
        super.stop();
        this.tableStatusCache = null;
        this.rowDefCache = null;
        this.curAIS = null;
        this.nameGenerator = null;
        this.memoryTableAIS = null;
    }

    @Override
    public void crash() {
        stop();
    }


    //
    // SchemaManager
    //

    @Override
    public void addOnlineChangeSet(Session session, ChangeSet changeSet) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        LOG.debug("addOnlineChangeSet: {} -> {}", onlineSession.id, changeSet);
        TransactionState txn = txnService.getTransaction(session);
        // Require existence
        DirectorySubspace onlineDir = smDirectory.open(txn.getTransaction(), onlineDirPath(onlineSession.id));
        // Create on demand
        DirectorySubspace changeDir = onlineDir.createOrOpen(txn.getTransaction(), CHANGES_TUPLE);
        byte[] packedKey = changeDir.pack(changeSet.getTableId());
        byte[] value = ChangeSetHelper.save(changeSet);
        txn.setBytes(packedKey, value);
    }


    //
    // AbstractSchemaManager
    //

    @Override
    protected NameGenerator getNameGenerator(Session session) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        return (getOnlineSession(session, null) != null) ?
            FDBNameGenerator.createForAlterPath(txn, rootDir, nameGenerator) :
            FDBNameGenerator.createForDataPath(txn, rootDir, nameGenerator);
    }

    @Override
    protected void storedAISChange(Session session,
                                   AkibanInformationSchema newAIS,
                                   Collection<String> schemaNames) {
        ByteBuffer buffer = null;
        validateAndFreeze(session, newAIS, null);
        TransactionState txn = txnService.getTransaction(session);
        for(String schema : schemaNames) {
            DirectorySubspace dir = smDirectory.createOrOpen(txn.getTransaction(), PROTOBUF_TUPLE);
            buffer = storeProtobuf(txn, dir, buffer, newAIS, schema);
        }
        buildRowDefCache(session, newAIS);
    }

    @Override
    protected void unStoredAISChange(Session session, final AkibanInformationSchema newAIS) {
        //
        // The *after* commit callback below is acceptable because in the real system, this
        // this method is only called during startup or shutdown and those are both single threaded.
        //
        // If that ever changes, that needs adjusted. However, the worst that can happen is a read
        // of the new system table (or routine, etc) after the commit but before the callback fires
        // This does not affect any user data whatsoever.
        //
        //
        // To be more strict and prevent that possibility, checks below would handle it but require test workarounds:
        //
        //ServiceManager.State state = serviceManager.getState();
        //if((state != ServiceManager.State.STARTING) && (state != ServiceManager.State.STOPPING)) {
        //    throw new IllegalStateException("Unexpected unSaved change: " + serviceManager.getState());
        //}

        // A new generation isn't needed as we evict the current copy below and, as above, single threaded startup
        validateAndFreeze(session, newAIS, null);
        buildRowDefCache(session, newAIS);

        txnService.addCallback(session, TransactionService.CallbackType.COMMIT, new TransactionService.Callback() {
            @Override
            public void run(Session session, long timestamp) {
                synchronized(AIS_LOCK) {
                    saveMemoryTables(newAIS);
                    FDBSchemaManager.this.curAIS = SENTINEL_AIS;
                }
            }
        });
    }

    @Override
    protected void clearTableStatus(Session session, Table table) {
        tableStatusCache.clearTableStatus(session, table);
    }

    @Override
    protected void bumpGeneration(Session session) {
        getNextGeneration(txnService.getTransaction(session));
    }

    @Override
    protected long generateSaveOnlineSessionID(Session session) {
        TransactionState txn = txnService.getTransaction(session);
        // New ID
        byte[] packedKey = smDirectory.pack(ONLINE_SESSION_KEY);
        byte[] value = txn.getTransaction().get(packedKey).get();
        long newID = (value == null) ? 1 : Tuple.fromBytes(value).getLong(0) + 1;
        txn.setBytes(packedKey, Tuple.from(newID).pack());
        // Create directory
        DirectorySubspace dir = smDirectory.create(txn.getTransaction(), onlineDirPath(newID));
        packedKey = dir.pack(GENERATION_KEY);
        value = Tuple.from(-1L).pack(); // No generation yet
        txn.setBytes(packedKey, value);
        return newID;
    }

    @Override
    protected void storedOnlineChange(Session session,
                                      OnlineSession onlineSession,
                                      AkibanInformationSchema newAIS,
                                      Collection<String> schemas) {
        // Get a unique generation for this AIS, but will only be visible to owning session
        validateAndFreeze(session, newAIS, null);
        // Again so no other transactions see the new one from validate
        bumpGeneration(session);
        // Save online schemas
        TransactionState txn = txnService.getTransaction(session);
        Tuple idPath = ONLINE_TUPLE.add(Long.toString(onlineSession.id));
        DirectorySubspace idDir = smDirectory.open(txn.getTransaction(), idPath);
        txn.setBytes(idDir.pack(GENERATION_KEY), Tuple.from(newAIS.getGeneration()).pack());
        DirectorySubspace protobufDir = idDir.createOrOpen(txn.getTransaction(), PROTOBUF_TUPLE);
        ByteBuffer buffer = null;
        for(String name : schemas) {
            buffer = storeProtobuf(txn, protobufDir, buffer, newAIS, name);
        }
    }

    @Override
    protected void clearOnlineState(Session session, OnlineSession onlineSession) {
        TransactionState txn = txnService.getTransaction(session);
        smDirectory.remove(txn.getTransaction(), onlineDirPath(onlineSession.id));
    }

    @Override
    protected OnlineCache buildOnlineCache(Session session) {
        OnlineCache onlineCache = new OnlineCache();

        Transaction txn = txnService.getTransaction(session).getTransaction();
        DirectorySubspace onlineDir = smDirectory.createOrOpen(txn, ONLINE_TUPLE);

        // For each online ID
        for(Object idStr : onlineDir.list(txn)) {
            assert (idStr instanceof String) : idStr;
            long onlineID = Long.parseLong((String)idStr);

            DirectorySubspace idDir = onlineDir.open(txn, Tuple.from(idStr));
            byte[] genBytes = txn.get(idDir.pack(GENERATION_KEY)).get();
            long generation = Tuple.fromBytes(genBytes).getLong(0);

            // load protobuf
            if(idDir.exists(txn, PROTOBUF_TUPLE)) {
                DirectorySubspace protobufDir = idDir.open(txn, PROTOBUF_TUPLE);
                int schemaCount = 0;
                for(KeyValue kv : txn.getRange(Range.startsWith(protobufDir.pack()))) {
                    Tuple keyTuple = Tuple.fromBytes(kv.getKey());
                    String schema = keyTuple.getString(keyTuple.size() - 1);
                    Long prev = onlineCache.schemaToOnline.put(schema, onlineID);
                    assert (prev == null) : String.format("%s, %d, %d", schema, prev, onlineID);
                    ++schemaCount;
                }
                if(generation != -1) {
                    ProtobufReader reader = newProtobufReader();
                    loadProtobufChildren(txn, protobufDir, reader, null);
                    loadPrimaryProtobuf(txn, reader, onlineCache.schemaToOnline.keySet());

                    // Reader will have two copies of affected schemas, skip second (i.e. non-online)
                    AkibanInformationSchema newAIS = finishReader(reader);
                    validateAndFreeze(session, newAIS, generation);
                    buildRowDefCache(session, newAIS);
                    onlineCache.onlineToAIS.put(onlineID, newAIS);
                } else if(schemaCount != 0) {
                    throw new IllegalStateException("No generation but had schemas");
                }
            }

            // Load ChangeSets
            if(idDir.exists(txn, CHANGES_TUPLE)) {
                DirectorySubspace changesDir = idDir.open(txn, CHANGES_TUPLE);
                for(KeyValue kv : txn.getRange(Range.startsWith(changesDir.pack()))) {
                    ChangeSet cs = ChangeSetHelper.load(kv.getValue());
                    Long prev = onlineCache.tableToOnline.put(cs.getTableId(), onlineID);
                    assert (prev == null) : String.format("%d, %d, %d", cs.getTableId(), prev, onlineID);
                    onlineCache.onlineToChangeSets.put(onlineID, cs);
                }
            }
        }

        return onlineCache;
    }

    @Override
    protected void renamingTable(Session session, TableName oldName, TableName newName) {
        Transaction txn = txnService.getTransaction(session).getTransaction();
        // Ensure destination schema exists. Can go away if schema lifetime becomes explicit.
        rootDir.createOrOpen(txn, FDBNameGenerator.dataPath(newName).popBack());
        rootDir.move(txn, FDBNameGenerator.dataPath(oldName), FDBNameGenerator.dataPath(newName));
    }

    @Override
    public AkibanInformationSchema getAis(Session session) {
        AkibanInformationSchema localAIS = session.get(SESSION_AIS_KEY);
        if(localAIS != null) {
            return localAIS;
        }
        TransactionState txn = txnService.getTransaction(session);
        long generation = getTransactionalGeneration(txn);
        localAIS = curAIS;
        if(generation != localAIS.getGeneration()) {
            synchronized(AIS_LOCK) {
                // May have been waiting
                if(generation == curAIS.getGeneration()) {
                    localAIS = curAIS;
                } else {
                    localAIS = loadFromStorage(session);
                    buildRowDefCache(session, localAIS);
                    if(localAIS.getGeneration() > curAIS.getGeneration()) {
                        curAIS = localAIS;
                        mergeNewAIS(curAIS);
                    }
                }
            }
        }
        OnlineSession onlineSession = getOnlineSession(session, null);
        if(onlineSession != null) {
            AkibanInformationSchema onlineAIS = getOnlineCache(session, localAIS).onlineToAIS.get(onlineSession.id);
            if(onlineAIS != null) {
                localAIS = onlineAIS;
            }
        }
        attachToSession(session, localAIS);
        return localAIS;
    }

    @Override
    public long getOldestActiveAISGeneration() {
        return curAIS.getGeneration();
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
        Transaction txn = txnService.getTransaction(session).getTransaction();
        rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(table.getName()));
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
        // None
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        // None
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
        // None
    }

     // TODO: Remove when FDB shutdown hook issue is resolved
     @Override
     public void unRegisterMemoryInformationSchemaTable(TableName tableName) {
         if(serviceManager.getState() == ServiceManager.State.STOPPING) {
             return; // Skip as to avoid DB access
         }
         super.unRegisterMemoryInformationSchemaTable(tableName);
     }

    // TODO: Remove when FDB shutdown hook issue is resolved
     @Override
     public void unRegisterSystemRoutine(TableName routineName) {
         if(serviceManager.getState() == ServiceManager.State.STOPPING) {
             return; // Skip as to avoid DB access
         }
         super.unRegisterSystemRoutine(routineName);
     }


    //
    // Helpers
    //

    private long getNextGeneration(TransactionState txn) {
        long newGeneration = getTransactionalGeneration(txn) + 1;
        saveGeneration(txn, newGeneration);
        return newGeneration;
    }

    private void saveGeneration(TransactionState txn, long newValue) {
        byte[] packedGen = Tuple.from(newValue).pack();
        txn.setBytes(packedGenKey, packedGen);
    }

    /** Validate and freeze {@code newAIS} at {@code generation} (or allocate a new one if {@code null}). */
    private void validateAndFreeze(Session session, AkibanInformationSchema newAIS, Long generation) {
        newAIS.validate(AISValidations.ALL_VALIDATIONS).throwIfNecessary();
        if(generation == null) {
            generation = getNextGeneration(txnService.getTransaction(session));
        }
        newAIS.setGeneration(generation);
        newAIS.freeze();
        attachToSession(session, newAIS);
    }

    private void saveMetaAndDataVersions(TransactionState txn) {
        // reads of data versions already done in loadAISFromStorage
        txn.setBytes(smDirectory.pack(DATA_VERSION_KEY), Tuple.from(CURRENT_DATA_VERSION).pack());
        txn.setBytes(smDirectory.pack(META_VERSION_KEY), Tuple.from(CURRENT_META_VERSION).pack());
    }

    private ByteBuffer storeProtobuf(TransactionState txn,
                                     DirectorySubspace dir,
                                     ByteBuffer buffer,
                                     AkibanInformationSchema newAIS,
                                     String schema) {
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

        byte[] packed = dir.pack(schema);
        if(newAIS.getSchema(schema) != null) {
            buffer = serialize(buffer, newAIS, selector);
            byte[] newValue = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
            txn.setBytes(packed, newValue);
        } else {
            txn.getTransaction().clear(packed);
        }
        return buffer;
    }

    private void saveMemoryTables(AkibanInformationSchema newAIS) {
        // Want *just* non-persisted memory tables and system routines
        this.memoryTableAIS = aisCloner.clone(newAIS, new ProtobufWriter.TableFilterSelector() {
            @Override
            public Columnar getSelected(Columnar columnar) {
                if(columnar.isTable() && ((Table)columnar).hasMemoryTableFactory()) {
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
        });
    }

    private void buildRowDefCache(Session session, AkibanInformationSchema newAIS) {
        tableStatusCache.detachAIS();
        rowDefCache.setAIS(session, newAIS);
    }

    private boolean checkDataVersions(TransactionState txn) {
        byte[] dataVerValue = txn.getTransaction().get(smDirectory.pack(DATA_VERSION_KEY)).get();
        byte[] metaVerValue = txn.getTransaction().get(smDirectory.pack(META_VERSION_KEY)).get();
        if(dataVerValue == null || metaVerValue == null) {
            assert dataVerValue == metaVerValue;
            return false;
        }
        long storedDataVer = Tuple.fromBytes(dataVerValue).getLong(0);
        long storedMetaVer = Tuple.fromBytes(metaVerValue).getLong(0);
        if((storedDataVer != CURRENT_DATA_VERSION) || (storedMetaVer != CURRENT_META_VERSION)) {
            throw new AkibanInternalException(String.format(
                "Unsupported meta,data versions: Stored(%d,%d) vs Current(%d,%d)",
                storedMetaVer,storedDataVer, CURRENT_META_VERSION,CURRENT_DATA_VERSION
            ));
        }
        return true;
    }

    private AkibanInformationSchema loadFromStorage(Session session) {
        TransactionState txn = txnService.getTransaction(session);
        checkDataVersions(txn);
        ProtobufReader reader = newProtobufReader();
        loadPrimaryProtobuf(txn.getTransaction(), reader, null);
        finishReader(reader);
        validateAndFreeze(session, reader.getAIS(), getTransactionalGeneration(txn));
        return reader.getAIS();
    }

    private void loadProtobufChildren(Transaction txn, Subspace dir, ProtobufReader reader, Collection<String> skip) {
        for(KeyValue kv : txn.getRange(Range.startsWith(dir.pack()))) {
            Tuple keyTuple = Tuple.fromBytes(kv.getKey());
            String schema = keyTuple.getString(keyTuple.size() - 1);
            if((skip != null) && skip.contains(schema)) {
                continue;
            }
            ByteBuffer buffer = ByteBuffer.wrap(kv.getValue());
            reader.loadBuffer(buffer);
        }
    }

    private void loadPrimaryProtobuf(Transaction txn, ProtobufReader reader, Collection<String> skipSchemas) {
        DirectorySubspace dir = smDirectory.createOrOpen(txn, PROTOBUF_TUPLE);
        loadProtobufChildren(txn, dir, reader, skipSchemas);
    }

    private long getTransactionalGeneration(TransactionState txn) {
        long generation = 0;
        byte[] packedGen = txn.getTransaction().get(packedGenKey).get();
        if(packedGen != null) {
            generation = Tuple.fromBytes(packedGen).getLong(0);
        }
        return generation;
    }

    private void mergeNewAIS(AkibanInformationSchema newAIS) {
        nameGenerator.mergeAIS(newAIS);
        tableVersionMap.claimExclusive();
        try {
            // Any number of changes, or recreations, may have happened to any table ID somewhere else.
            // The new, transactional state is in newAIS *only*. Take that as the sole source.
            tableVersionMap.clear();
            for(Table table : newAIS.getTables().values()) {
                tableVersionMap.put(table.getTableId(), table.getVersion());
            }
        } finally {
            tableVersionMap.releaseExclusive();
        }
    }

    private void attachToSession(Session session, AkibanInformationSchema ais) {
        AkibanInformationSchema prev = session.put(SESSION_AIS_KEY, ais);
        if(prev == null) {
            txnService.addCallback(session, TransactionService.CallbackType.END, CLEAR_SESSION_KEY_CALLBACK);
        }
    }

    private ProtobufReader newProtobufReader() {
        // Start with existing memory tables, merge in stored ones
        final AkibanInformationSchema newAIS = aisCloner.clone(memoryTableAIS);
        return new ProtobufReader(storageFormatRegistry, newAIS);
    }

    //
    // Static helpers
    //

    private static AkibanInformationSchema finishReader(ProtobufReader reader) {
        reader.loadAIS();
        for(Table table : reader.getAIS().getTables().values()) {
            // nameGenerator is only needed to generate hidden PK, which shouldn't happen here
            table.endTable(null);
        }
        return reader.getAIS();
    }

    private static Tuple onlineDirPath(long onlineID) {
        return ONLINE_TUPLE.add(Long.toString(onlineID));
    }

    /** Serialize given AIS. Allocates a new buffer if necessary so always use <i>returned</i> buffer. */
    private static ByteBuffer serialize(ByteBuffer buffer, AkibanInformationSchema ais, ProtobufWriter.WriteSelector selector) {
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

    private static final TransactionService.Callback CLEAR_SESSION_KEY_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(SESSION_AIS_KEY);
        }
    };
}
