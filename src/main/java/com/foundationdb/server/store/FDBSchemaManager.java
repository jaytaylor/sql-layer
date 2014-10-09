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

import com.foundationdb.ais.model.AbstractVisitor;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.SQLJJar;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.blob.BlobAsync;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.directory.PathUtil;
import com.foundationdb.qp.storeadapter.FDBAdapter;
import com.foundationdb.server.FDBTableStatusCache;
import com.foundationdb.server.error.FDBAdapterException;
import com.foundationdb.server.error.MetadataVersionNewerException;
import com.foundationdb.server.error.MetadataVersionTooOldException;
import com.foundationdb.server.rowdata.RowDefBuilder;
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
import com.foundationdb.server.types.service.TypesRegistryService;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.ByteArrayUtil;
import com.foundationdb.tuple.Tuple2;
import com.google.inject.Inject;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

/**
 * Directory usage:
 * <pre>
 * root_dir/
 *   schemaManager/
 *     online/
 *       id/
 *         dml/
 *           tid/           => hKeys of concurrent DML
 *         protobuf/
 *           schema_name    => byte[] (AIS Protobuf)
 *         changes/
 *           tid            => byte[] (ChangeSet Protobuf)
 *         generation       => long   (session's generation)
 *         error            => string (error message, only set on error)
 *     protobuf/
 *       schema_name/       => byte[] (AIS Protobuf)
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

    static final String CLEAR_INCOMPATIBLE_DATA_PROP = "fdbsql.fdb.clear_incompatible_data";
    static final String EXTERNAL_CLEAR_MSG = "SQL Layer metadata has been externally modified. Restart required.";
    static final String EXTERNAL_VER_CHANGE_MSG = "SQL Layer version has been changed from another node.";

    private static final List<String> SCHEMA_MANAGER_PATH = Arrays.asList("schemaManager");
    private static final List<String> PROTOBUF_PATH = Arrays.asList("protobuf");
    private static final List<String> ONLINE_PATH = Arrays.asList("online");
    private static final List<String> CHANGES_PATH = Arrays.asList("changes");
    private static final List<String> DML_PATH = Arrays.asList("dml");
    private static final String GENERATION_KEY = "generation";
    private static final String DATA_VERSION_KEY = "dataVersion";
    private static final String META_VERSION_KEY = "metaDataVersion";
    private static final String ONLINE_SESSION_KEY = "onlineSession";
    private static final String ERROR_KEY = "error";

    /**
     * 1) Initial
     * 2) Fixed charset width computation
     * 3) No long string digest in indexes
     * 4) Unique index format change
     * 5) Remove group index row counts
     * 6) Metadata stored using blob layer
     */
    private static final long CURRENT_DATA_VERSION = 6;
    /**
     * 1) Initial directory based
     * 2) Online metadata support
     * 3) Type bundles
     * 4) Online DDL error-ing
     * 5) ????
     * 6) ????
     * 7) Hidden PK to Sequence/__row_id
     */
    private static final long CURRENT_META_VERSION = 7;

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
    private byte[] packedDataVerKey;
    private byte[] packedMetaVerKey;
    private FDBTableStatusCache tableStatusCache;
    private AkibanInformationSchema curAIS;
    private NameGenerator nameGenerator;
    private AkibanInformationSchema memoryTableAIS;


    @Inject
    public FDBSchemaManager(ConfigurationService config,
                            SessionService sessionService,
                            FDBHolder holder,
                            TransactionService txnService,
                            ListenerService listenerService,
                            ServiceManager serviceManager,
                            TypesRegistryService typesRegistryService) {
        super(config, sessionService, txnService, typesRegistryService, new FDBStorageFormatRegistry(config));
        this.holder = holder;
        if(txnService instanceof FDBTransactionService) {
            this.txnService = (FDBTransactionService)txnService;
        } else {
            throw new IllegalStateException("May only be used with FDBTransactionService");
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
        final boolean clearIncompatibleData = Boolean.parseBoolean(config.getProperty(CLEAR_INCOMPATIBLE_DATA_PROP));

        initSchemaManagerDirectory();
        this.memoryTableAIS = new AkibanInformationSchema();
        this.tableStatusCache = new FDBTableStatusCache(holder, txnService);

        try(Session session = sessionService.createSession()) {
            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    TransactionState txn = txnService.getTransaction(session);
                    Boolean isCompatible = isDataCompatible(txn, false);
                    if(isCompatible == Boolean.FALSE) {
                        if(!clearIncompatibleData) {
                            isDataCompatible(txn, true);
                            assert false; // Throw expected
                        }
                        LOG.warn("Clearing incompatible data directory: {}", rootDir.getPath());
                        // Delicate: Directory removal is safe as this is the first service started that consumes it.
                        //           Remove after the 1.9.2 release, which includes entry point for doing this.
                        rootDir.remove(txn.getTransaction()).get();
                        initSchemaManagerDirectory();
                        isCompatible = null;
                    }
                    if(isCompatible == null) {
                        saveInitialState(txn);
                    }
                    AkibanInformationSchema newAIS = loadFromStorage(session);
                    buildRowDefs(session, newAIS);
                    FDBSchemaManager.this.curAIS = newAIS;
                }
            });

            this.nameGenerator = new DefaultNameGenerator(curAIS);

            txnService.run(session, new Runnable() {
                @Override
                public void run() {
                    mergeNewAIS(session, curAIS);
                }
            });
        }

        listenerService.registerTableListener(this);

        registerSystemTables();
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
        super.stop();
        this.tableStatusCache = null;
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
    // Called through BasicDDLFunctions, but only via 
    // TransactionService.run(Session, Runnable), which handles the
    // Exceptions

    @Override
    public void addOnlineChangeSet(Session session, ChangeSet changeSet) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        LOG.debug("addOnlineChangeSet: {} -> {}", onlineSession.id, changeSet);
        onlineSession.tableIDs.add(changeSet.getTableId());
        TransactionState txn = txnService.getTransaction(session);
        // Require existence
        DirectorySubspace onlineDir = openDirectory (txn, smDirectory, onlineDirPath(onlineSession.id)); 
        // Create on demand
       DirectorySubspace changeDir = onlineDir.createOrOpen(txn.getTransaction(), CHANGES_PATH).get();
        byte[] packedKey = changeDir.pack(changeSet.getTableId());
        byte[] value = ChangeSetHelper.save(changeSet);
        txn.setBytes(packedKey, value);
        // TODO: Cleanup into Abstract. For consistency with PSSM.
        if(getAis(session).getGeneration() == getOnlineAIS(session).getGeneration()) {
            bumpGeneration(session);
        }
    }

    @Override
    public Set<String> getTreeNames(final Session session) {
        return txnService.run(session, new Callable<Set<String>>() {
            @Override
            public Set<String> call() {
                AkibanInformationSchema ais = getAis(session);
                StorageNameVisitor visitor = new StorageNameVisitor();
                ais.visit(visitor);
                for(Sequence s : ais.getSequences().values()) {
                    visitor.visit(s);
                }
                return visitor.pathNames;
            }
        });
    }


    //
    // AbstractSchemaManager
    //

    @Override
    protected NameGenerator getNameGenerator(Session session) {
        return (getOnlineSession(session, null) != null) ?
            FDBNameGenerator.createForOnlinePath(txnService.getTransaction(session), rootDir, nameGenerator) :
            FDBNameGenerator.createForDataPath(txnService.getTransaction(session), rootDir, nameGenerator);
    }

    @Override
    protected void storedAISChange(Session session,
                                   AkibanInformationSchema newAIS,
                                   Collection<String> schemaNames) {
        ByteBuffer buffer = null;
        validateForSession(session, newAIS, null);
        try {
            TransactionState txn = txnService.getTransaction(session);
            for(String schema : schemaNames) {
                DirectorySubspace dir = smDirectory.createOrOpen(txn.getTransaction(), PROTOBUF_PATH).get();
                buffer = storeProtobuf(txn, dir, buffer, newAIS, schema);
            }
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
        buildRowDefs(session, newAIS);
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
        validateForSession(session, newAIS, null);
        buildRowDefs(session, newAIS);

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
        getNextGeneration(session, txnService.getTransaction(session));
    }

    @Override
    protected long generateSaveOnlineSessionID(Session session) {
        TransactionState txn = txnService.getTransaction(session);
        // New ID
        byte[] packedKey = smDirectory.pack(ONLINE_SESSION_KEY);
        byte[] value = txn.getValue(packedKey); 
        long newID = (value == null) ? 1 : Tuple2.fromBytes(value).getLong(0) + 1;
        txn.setBytes(packedKey, Tuple2.from(newID).pack());
        // Create directory
        DirectorySubspace dir = createDirectory(txn, smDirectory,onlineDirPath(newID));
        packedKey = dir.pack(GENERATION_KEY);
        value = Tuple2.from(-1L).pack(); // No generation yet
        txn.setBytes(packedKey, value);
        return newID;
    }

    @Override
    protected void storedOnlineChange(Session session,
                                      OnlineSession onlineSession,
                                      AkibanInformationSchema newAIS,
                                      Collection<String> schemas) {
        // Get a unique generation for this AIS, but will only be visible to owning session
        validateForSession(session, newAIS, null);
        // Again so no other transactions see the new one from validate
        bumpGeneration(session);
        // Save online schemas
        TransactionState txn = txnService.getTransaction(session);
        List<String> idPath = onlineDirPath(onlineSession.id);
        DirectorySubspace idDir = openDirectory(txn, smDirectory, idPath);
        txn.setBytes(idDir.pack(GENERATION_KEY), Tuple2.from(newAIS.getGeneration()).pack());
        
        try {
            DirectorySubspace protobufDir = idDir.createOrOpen(txn.getTransaction(), PROTOBUF_PATH).get();
            ByteBuffer buffer = null;
            for(String name : schemas) {
                buffer = storeProtobuf(txn, protobufDir, buffer, newAIS, name);
            }
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
    }

    @Override
    protected void clearOnlineState(Session session, OnlineSession onlineSession) {
        try {
            TransactionState txn = txnService.getTransaction(session);
            smDirectory.remove(txn.getTransaction(), onlineDirPath(onlineSession.id)).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
    }

    @Override
    protected OnlineCache buildOnlineCache(Session session) {
        OnlineCache onlineCache = new OnlineCache();

        TransactionState txnState = txnService.getTransaction(session); 
        Transaction txn = txnState.getTransaction();
        
        try {
            
            DirectorySubspace onlineDir = smDirectory.createOrOpen(txn, ONLINE_PATH).get();
    
            // For each online ID
            for(String idStr : onlineDir.list(txn).get()) {
                long onlineID = Long.parseLong(idStr);
                DirectorySubspace idDir = onlineDir.open(txn, Arrays.asList(idStr)).get();
                byte[] genBytes = txnState.getValue(idDir.pack(GENERATION_KEY));
                long generation = Tuple2.fromBytes(genBytes).getLong(0);
    
                // load protobuf
                if(idDir.exists(txn, PROTOBUF_PATH).get()) {
                    DirectorySubspace protobufDir = idDir.open(txn, PROTOBUF_PATH).get();
                    int schemaCount = 0;
                    for(String schema : protobufDir.list(txn).get()) {
                        Long prev = onlineCache.schemaToOnline.put(schema, onlineID);
                        assert (prev == null) : String.format("%s, %d, %d", schema, prev, onlineID);
                        ++schemaCount;
                    }
                    if(generation != -1) {
                        ProtobufReader reader = newProtobufReader();
                        loadProtobufChildren(txnState, protobufDir, reader, null);
                        loadPrimaryProtobuf(txnState, reader, onlineCache.schemaToOnline.keySet());
    
                        // Reader will have two copies of affected schemas, skip second (i.e. non-online)
                        AkibanInformationSchema newAIS = finishReader(reader);
                        validateAndFreeze(session, newAIS, generation);
                        buildRowDefs(session, newAIS);
                        onlineCache.onlineToAIS.put(onlineID, newAIS);
                    } else if(schemaCount != 0) {
                        throw new IllegalStateException("No generation but had schemas");
                    }
                }
    
                // Load ChangeSets
                if(idDir.exists(txn, CHANGES_PATH).get()) {
                    DirectorySubspace changesDir = idDir.open(txn, CHANGES_PATH).get();
                    for(KeyValue kv : txn.getRange(Range.startsWith(changesDir.pack()))) {
                        ChangeSet cs = ChangeSetHelper.load(kv.getValue());
                        Long prev = onlineCache.tableToOnline.put(cs.getTableId(), onlineID);
                        assert (prev == null) : String.format("%d, %d, %d", cs.getTableId(), prev, onlineID);
                        onlineCache.onlineToChangeSets.put(onlineID, cs);
                    }
                }
            }
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
        return onlineCache;
    }

    @Override
    protected void newTableVersions(Session session, Map<Integer, Integer> versions) {
        // None
    }

    @Override
    protected void renamingTable(Session session, TableName oldName, TableName newName) {
        try {
            Transaction txn = txnService.getTransaction(session).getTransaction();
            // Ensure destination schema exists. Can go away if schema lifetime becomes explicit.
            rootDir.createOrOpen(txn, PathUtil.popBack(FDBNameGenerator.dataPath(newName))).get();
            rootDir.move(txn, FDBNameGenerator.dataPath(oldName), FDBNameGenerator.dataPath(newName)).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
    }

    @Override 
    public AkibanInformationSchema getSessionAIS(Session session) {
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
                    buildRowDefs(session, localAIS);
                    if(localAIS.getGeneration() > curAIS.getGeneration()) {
                        curAIS = localAIS;
                        mergeNewAIS(session, curAIS);
                    }
                }
            }
        }
        attachToSession(session, localAIS);
        return localAIS;
    }

    @Override
    public long getOldestActiveAISGeneration() {
        return curAIS.getGeneration();
    }

    @Override
    public Set<Long> getActiveAISGenerations() {
        return Collections.singleton(curAIS.getGeneration());
    }

    @Override
    public boolean hasTableChanged(Session session, int tableID) {
        // Handled by serializable transactions
        return false;
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
        try {
            Transaction txn = txnService.getTransaction(session).getTransaction();
            rootDir.removeIfExists(txn, FDBNameGenerator.dataPath(table.getName())).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(session, e);
        }
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

    @Override
    public void addOnlineHandledHKey(Session session, int tableID, Key hKey) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        if(onlineID == null) {
            throw new IllegalArgumentException("No online change for table: " + tableID);
        }
        TransactionState txn = txnService.getTransaction(session);
        DirectorySubspace tableDMLDir = getOnlineTableDMLDir(txn, onlineID, tableID);
        byte[] hKeyBytes = Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize());
        byte[] packedKey = tableDMLDir.pack(Tuple2.from(hKeyBytes));
        txn.setBytes(packedKey, new byte[0]);
    }

    @Override
    public void setOnlineDMLError(Session session, int tableID, String message) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        if(onlineID == null) {
            throw new IllegalArgumentException("No online change for table: " + tableID);
        }
        TransactionState txn = txnService.getTransaction(session);
        DirectorySubspace onlineDir = getOnlineDir(txn, onlineID);
        byte[] packedKey = onlineDir.pack(ERROR_KEY);
        byte[] packedValue = Tuple2.from(message).pack();
        txn.setBytes(packedKey, packedValue);
    }

    @Override
    public String getOnlineDMLError(Session session) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        TransactionState txn = txnService.getTransaction(session);
        DirectorySubspace dir = getOnlineDir(txn, onlineSession.id);
        byte[] value = txn.getValue(dir.pack(ERROR_KEY));
        return (value == null) ? null : Tuple2.fromBytes(value).getString(0);
    }

    @Override
    public Iterator<byte[]> getOnlineHandledHKeyIterator(Session session, int tableID, Key hKey) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        if(LOG.isDebugEnabled()) {
            LOG.debug("addOnlineHandledHKey: {}/{} -> {}", new Object[] { onlineSession.id, tableID, hKey });
        }
        TransactionState txn = txnService.getTransaction(session);
        DirectorySubspace tableDMLDir = getOnlineTableDMLDir(txn, onlineSession.id, tableID);
        byte[] startKey = tableDMLDir.pack();
        byte[] endKey = ByteArrayUtil.strinc(startKey);
        if(hKey != null) {
            startKey = ByteArrayUtil.join(tableDMLDir.pack(), Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize()));
        }
        final Iterator<KeyValue> iterator = txn.getRangeIterator(startKey, endKey);
        final int prefixLength = tableDMLDir.pack().length;
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[] next() {
                if(!iterator.hasNext()) {
                    return null;
                }
                byte[] keyBytes = iterator.next().getKey();
                return Arrays.copyOfRange(keyBytes, prefixLength, keyBytes.length);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }


    //
    // Helpers
    //

    private DirectorySubspace openDirectory (TransactionState txn, DirectorySubspace dir, List<String> dirs) {
        try {
            return dir.open(txn.getTransaction(), dirs).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(txn.session, e);
        }
    }
    
    private DirectorySubspace createDirectory (TransactionState txn, DirectorySubspace dir, List<String>dirs) {
        try {
            // Create directory
           return dir.create(txn.getTransaction(), dirs).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(txn.session, e);
        }
    }

    private void initSchemaManagerDirectory() {
        rootDir = holder.getRootDirectory();
        smDirectory = rootDir.createOrOpen(holder.getDatabase(), SCHEMA_MANAGER_PATH).get();
        // Cache as this is checked on every transaction
        packedGenKey = smDirectory.pack(GENERATION_KEY);
        // And these are checked for every AIS load
        packedDataVerKey = smDirectory.pack(DATA_VERSION_KEY);
        packedMetaVerKey = smDirectory.pack(META_VERSION_KEY);
    }

    private long getNextGeneration(Session session, TransactionState txn) {
        long newGeneration = getTransactionalGeneration(txn) + 1;
        saveGeneration(txn, newGeneration);
        return newGeneration;
    }

    private void saveGeneration(TransactionState txn, long newValue) {
        byte[] packedGen = Tuple2.from(newValue).pack();
        txn.setBytes(packedGenKey, packedGen);
    }

    /** Validate and freeze {@code newAIS} at {@code generation} (or allocate a new one if {@code null}). */
    private void validateAndFreeze(Session session, AkibanInformationSchema newAIS, Long generation) {
        newAIS.validate(AISValidations.ALL_VALIDATIONS).throwIfNecessary();
        if(generation == null) {
            generation = getNextGeneration(session, txnService.getTransaction(session));
        }
        newAIS.setGeneration(generation);
        newAIS.freeze();
    }

    /** {@link #validateAndFreeze} and {@link #attachToSession}. For AISs {@code session} should continue to see. */
    private void validateForSession(Session session, AkibanInformationSchema newAIS, Long generation) {
        validateAndFreeze(session, newAIS, generation);
        attachToSession(session, newAIS);
    }

    private void saveInitialState(TransactionState txn) {
        txn.setBytes(packedDataVerKey, Tuple2.from(CURRENT_DATA_VERSION).pack());
        txn.setBytes(packedMetaVerKey, Tuple2.from(CURRENT_META_VERSION).pack());
        txn.setBytes(packedGenKey, Tuple2.from(0).pack());
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

        if(newAIS.getSchema(schema) != null) {
            Subspace blobDir = dir.createOrOpen(txn.getTransaction(), PathUtil.from(schema)).get();
            buffer = serialize(buffer, newAIS, selector);
            byte[] newValue;
            if((buffer.position() == 0) && (buffer.limit() == buffer.capacity())) {
                newValue = buffer.array();
            } else {
                newValue = Arrays.copyOfRange(buffer.array(), buffer.position(), buffer.limit());
            }
            BlobAsync blob = new BlobAsync(blobDir);
            blob.truncate(txn.getTransaction(), 0).get();
            blob.write(txn.getTransaction(), 0, newValue).get();
        } else {
            dir.removeIfExists(txn.getTransaction(), PathUtil.from(schema)).get();
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
        });
    }

    private void buildRowDefs(Session session, AkibanInformationSchema newAIS) {
        tableStatusCache.detachAIS();
        RowDefBuilder rowDefBuilder = new RowDefBuilder(session, newAIS, tableStatusCache);
        rowDefBuilder.build();
    }

    /** {@code null} = no data present, {@code true} = compatible, {@code false} = incompatible */
    private Boolean isDataCompatible(TransactionState txn, boolean throwIfIncompatible) {
        byte[] dataVerValue = txn.getValue(packedDataVerKey);
        byte[] metaVerValue = txn.getValue(packedMetaVerKey);
        if(dataVerValue == null || metaVerValue == null) {
            return null;
        }
        long storedDataVer = Tuple2.fromBytes(dataVerValue).getLong(0);
        long storedMetaVer = Tuple2.fromBytes(metaVerValue).getLong(0);
        if((storedDataVer != CURRENT_DATA_VERSION) || (storedMetaVer != CURRENT_META_VERSION)) {
            if(throwIfIncompatible) {
                if ((storedDataVer >= CURRENT_DATA_VERSION) || (storedMetaVer >= CURRENT_META_VERSION)) {
                    throw new MetadataVersionNewerException (CURRENT_META_VERSION, CURRENT_DATA_VERSION, storedMetaVer, storedDataVer);
                } else {
                    throw new MetadataVersionTooOldException(CURRENT_META_VERSION, CURRENT_DATA_VERSION, storedMetaVer, storedDataVer);
                }
            }
            return Boolean.FALSE;
        }
        return Boolean.TRUE;
    }

    private void checkDataVersions(TransactionState txn) {
        Boolean isCompatible = isDataCompatible(txn, false);
        // Can only be missing if clear()-ed outside SQL Layer. Give clear message but no recovery attempt.
        if(isCompatible == null) {
            throw new FDBAdapterException(EXTERNAL_CLEAR_MSG);
        }
        if(isCompatible == Boolean.FALSE) {
            throw new FDBAdapterException(EXTERNAL_VER_CHANGE_MSG);
        }
        assert isCompatible;
    }

    private AkibanInformationSchema loadFromStorage(Session session) {
        TransactionState txn = txnService.getTransaction(session);
        checkDataVersions(txn);
        ProtobufReader reader = newProtobufReader();
        loadPrimaryProtobuf(txn, reader, null);
        finishReader(reader);
        validateAndFreeze(session, reader.getAIS(), getTransactionalGeneration(txn));
        return reader.getAIS();
    }

    private void loadProtobufChildren(TransactionState txn, DirectorySubspace dir, ProtobufReader reader, Collection<String> skip) {
        for(String subDirName : dir.list(txn.getTransaction()).get()) {
            if((skip != null) && skip.contains(subDirName)) {
                continue;
            }
            Subspace subDir = dir.open(txn.getTransaction(), PathUtil.from(subDirName)).get();
            BlobAsync blob = new BlobAsync(subDir);
            byte[] data = blob.read(txn.getTransaction()).get();
            if(data != null) {
                ByteBuffer buffer = ByteBuffer.wrap(data);
                reader.loadBuffer(buffer);
            }
        }
    }

    private void loadPrimaryProtobuf(TransactionState txn, ProtobufReader reader, Collection<String> skipSchemas) {
        DirectorySubspace dir = smDirectory.createOrOpen(txn.getTransaction(), PROTOBUF_PATH).get();
        loadProtobufChildren(txn, dir, reader, skipSchemas);
    }

    private long getTransactionalGeneration(TransactionState txn) {
        byte[] packedGen;
        packedGen = txn.getValue(packedGenKey);
        if(packedGen == null) {
            throw new FDBAdapterException(EXTERNAL_CLEAR_MSG);
        }
        return Tuple2.fromBytes(packedGen).getLong(0);
    }

    private void mergeNewAIS(Session session, AkibanInformationSchema newAIS) {
        OnlineCache onlineCache = getOnlineCache(session, newAIS);
        nameGenerator.mergeAIS(newAIS);
        for(AkibanInformationSchema onlineAIS : onlineCache.onlineToAIS.values()) {
            nameGenerator.mergeAIS(onlineAIS);
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
        return new ProtobufReader(typesRegistryService.getTypesRegistry(), storageFormatRegistry, newAIS);
    }

    private DirectorySubspace getOnlineDir(TransactionState txn, long onlineID) {
        try {
            // Require existence
            return smDirectory.open(txn.getTransaction(), onlineDirPath(onlineID)).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(txn.session, e);
        }
    }

    private DirectorySubspace getOnlineTableDMLDir(TransactionState txn, long onlineID, int tableID) {
        try {
            // Create on demand
            return getOnlineDir(txn, onlineID).createOrOpen(txn.getTransaction(), PathUtil.extend(DML_PATH, String.valueOf(tableID))).get();
        } catch (RuntimeException e) {
            throw FDBAdapter.wrapFDBException(txn.session, e);
        }
    }

    //
    // Test helpers
    //

    byte[] getPackedGenKey() {
        return packedGenKey;
    }

    byte[] getPackedDataVerKey() {
        return packedDataVerKey;
    }

    byte[] getPackedMetaVerKey() {
        return packedMetaVerKey;
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

    private static List<String> onlineDirPath(long onlineID) {
        return PathUtil.extend(ONLINE_PATH, Long.toString(onlineID));
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

    /** Collect all StorageDescriptions into a test/debug friendly set of path descriptions. */
    private static class StorageNameVisitor extends AbstractVisitor {
        public final Set<String> pathNames = new TreeSet<>();

        @Override
        public void visit(Group group) {
            track(FDBNameGenerator.dataPath(group.getName()));
        }

        @Override
        public void visit(Table table) {
            track(FDBNameGenerator.dataPath(table.getName()));
        }

        @Override
        public void visit(Index index) {
            track(FDBNameGenerator.dataPath(index));
        }

        public void visit(Sequence sequence) {
            track(FDBNameGenerator.dataPath(sequence));
        }

        private void track(List<String> path) {
            pathNames.add(path.toString());
        }
    }

    private static final TransactionService.Callback CLEAR_SESSION_KEY_CALLBACK = new TransactionService.Callback() {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(SESSION_AIS_KEY);
        }
    };
}
