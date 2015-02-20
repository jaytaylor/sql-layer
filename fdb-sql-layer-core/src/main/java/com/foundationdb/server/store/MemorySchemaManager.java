/**
 * Copyright (C) 2009-2015 FoundationDB, LLC
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
import com.foundationdb.ais.model.DefaultNameGenerator;
import com.foundationdb.ais.model.Group;
import com.foundationdb.ais.model.HasStorage;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.NameGenerator;
import com.foundationdb.ais.model.Sequence;
import com.foundationdb.ais.model.StorageDescription;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.validation.AISValidations;
import com.foundationdb.ais.protobuf.ProtobufReader;
import com.foundationdb.ais.protobuf.ProtobufWriter;
import com.foundationdb.qp.virtualadapter.VirtualAdapter;
import com.foundationdb.server.TableStatus;
import com.foundationdb.server.MemoryTableStatusCache;
import com.foundationdb.server.rowdata.RowDefBuilder;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.transaction.TransactionService.Callback;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.MemoryStorageDescription;
import com.foundationdb.server.store.format.MemoryStorageFormatRegistry;
import com.foundationdb.server.types.service.TypesRegistryService;
import com.google.inject.Inject;
import com.persistit.Key;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.Callable;

import static com.foundationdb.server.store.MemoryStore.BYTES_00;
import static com.foundationdb.server.store.MemoryStore.BYTES_EMPTY;
import static com.foundationdb.server.store.MemoryStore.BYTES_FF;
import static com.foundationdb.server.store.MemoryStore.join;
import static com.foundationdb.server.store.MemoryStore.packLong;
import static com.foundationdb.server.store.MemoryStore.packUUID;
import static com.foundationdb.server.store.MemoryStore.unpackLong;
import static com.foundationdb.server.store.MemoryStore.unpackUUID;

/**
 * "Subspace" layout:
 * smBytes/
 *   "online"/
 *     online_id/
 *       "changes"/
 *         table_id         => [ChangeSet protobuf]
 *       "error"            => [error message UTF-8]
 *       "generation"       => [generation long]
 *       "hkeys"/
 *         table_id/
 *           hkey_bytes       => []
 *       "protobuf"/
 *         schema_name      => [AIS protobuf]
 *   "online_session_id"    => [session id long]
 *   "protobuf
 * tsBytes/
 *   pkBytes                => [row count long]
 */
public class MemorySchemaManager extends AbstractSchemaManager
{
    private static final Session.Key<AkibanInformationSchema> SESSION_AIS_KEY = Session.Key.named("AIS");

    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final byte[] CHANGES_STR_BYTES = "changes".getBytes(UTF8);
    private static final byte[] ERROR_STR_BYTES = "error".getBytes(UTF8);
    private static final byte[] GENERATION_STR_BYTES = "generation".getBytes(UTF8);
    private static final byte[] HKEYS_STR_BYTES = "hkeys".getBytes(UTF8);
    private static final byte[] ONLINE_STR_BYTES = "online".getBytes(UTF8);
    private static final byte[] ONLINE_SESSION_ID_STR_BYTES = "online_session_id".getBytes(UTF8);
    private static final byte[] PROTOBUF_STR_BYTES = "protobuf".getBytes(UTF8);

    private final MemoryTransactionService txnService;
    private final byte[] smBytes = packUUID(UUID.randomUUID());
    private final byte[] tsBytes = packUUID(UUID.randomUUID());

    private volatile MemoryTableStatusCache tableStatusCache;
    private volatile AkibanInformationSchema curAIS;
    private volatile NameGenerator nameGenerator;

    @Inject
    public MemorySchemaManager(ConfigurationService config,
                               SessionService sessionService,
                               TransactionService txnService,
                               TypesRegistryService typesRegistryService) {
        super(config, sessionService, txnService, typesRegistryService, new MemoryStorageFormatRegistry(config));
        this.txnService = (MemoryTransactionService)txnService;
    }

    //
    // Service
    //

    @Override
    public void start() {
        super.start();
        tableStatusCache = new MemoryTableStatusCache(txnService, tsBytes);
        nameGenerator = new DefaultNameGenerator();

    }

    @Override
    public void stop() {
        nameGenerator = null;
        curAIS = null;
        tableStatusCache = null;
        super.stop();
    }

    @Override
    public void crash() {
        stop();
    }

    //
    // AbstractSchemaManager
    //

    @Override
    protected NameGenerator getNameGenerator(Session session) {
        return nameGenerator;
    }

    @Override
    protected AkibanInformationSchema getSessionAIS(Session session) {
        MemoryTransaction txn = getTransaction(session);
        AkibanInformationSchema localAIS = session.get(SESSION_AIS_KEY);
        if(localAIS != null) {
            return localAIS;
        }
        localAIS = curAIS;
        if(localAIS == null) {
            synchronized(this) {
                if(curAIS == null) {
                    curAIS = loadFromStorage(txn);
                    nameGenerator.mergeAIS(curAIS);
                }
                localAIS = curAIS;
            }
        }
        long localGen = getCurrentAISGeneration(txn);
        if(localGen != localAIS.getGeneration()) {
            localAIS = loadFromStorage(txn);
            synchronized(this) {
                if(localAIS.getGeneration() > curAIS.getGeneration()) {
                    curAIS = localAIS;
                }
            }
        }
        attachToSession(session, localAIS);
        return localAIS;
    }

    @Override
    protected void storedAISChange(Session session, AkibanInformationSchema newAIS, Collection<String> schemas) {
        MemoryTransaction txn = getTransaction(session);
        validateAndFreeze(txn, newAIS, null);
        attachToSession(session, newAIS);
        for(String schema : schemas) {
            storeProtobuf(txn, join(smBytes, PROTOBUF_STR_BYTES), newAIS, schema);
        }
        buildRowDefs(newAIS);
    }

    @Override
    protected void unStoredAISChange(Session session, AkibanInformationSchema newAIS) {
        // Treat as normal change as everything goes away on restart anyway.
        storedAISChange(session,
                        newAIS,
                        Arrays.asList(TableName.INFORMATION_SCHEMA,
                                      TableName.SECURITY_SCHEMA,
                                      TableName.SQLJ_SCHEMA,
                                      TableName.SYS_SCHEMA));
    }

    @Override
    protected void renamingTable(Session session, TableName oldName, TableName newName) {
        // None
    }

    @Override
    protected void clearTableStatus(Session session, Table table) {
        tableStatusCache.clearTableStatus(session, table);
    }

    @Override
    protected void bumpGeneration(Session session) {
        MemoryTransaction txn = getTransaction(session);
        getNextAISGeneration(txn);
    }

    @Override
    protected long generateSaveOnlineSessionID(Session session) {
        MemoryTransaction txn = getTransaction(session);
        byte[] key = join(smBytes, ONLINE_SESSION_ID_STR_BYTES);
        byte[] value = txn.get(key);
        long nextID = 1;
        if(value != null) {
            nextID = unpackLong(value) + 1;
        }
        txn.set(key, packLong(nextID));
        return nextID;
    }

    @Override
    protected void storedOnlineChange(Session session,
                                      OnlineSession onlineSession,
                                      AkibanInformationSchema newAIS,
                                      Collection<String> schemas) {
        MemoryTransaction txn = getTransaction(session);
        // Get a unique generation for this AIS, but will only be visible to owning session
        validateAndFreeze(txn, newAIS, null);
        attachToSession(session, newAIS);
        // Again so no other transactions see the new one from validate
        bumpGeneration(session);
        // Save online id
        byte[] sessionBytes = join(smBytes, ONLINE_STR_BYTES, packLong(onlineSession.id));
        txn.set(sessionBytes, BYTES_EMPTY);
        // Save online schemas
        txn.set(join(sessionBytes, GENERATION_STR_BYTES), packLong(newAIS.getGeneration()));
        byte[] protobufBytes = join(sessionBytes, PROTOBUF_STR_BYTES);
        for(String name : schemas) {
            storeProtobuf(txn, protobufBytes, newAIS, name);
        }
    }

    @Override
    protected void clearOnlineState(Session session, OnlineSession onlineSession) {
        MemoryTransaction txn = getTransaction(session);
        byte[] sessionBytes = join(smBytes, ONLINE_STR_BYTES, packLong(onlineSession.id));
        txn.clearRange(sessionBytes, join(sessionBytes, BYTES_FF));
    }

    @Override
    protected OnlineCache buildOnlineCache(Session session) {
        MemoryTransaction txn = getTransaction(session);
        OnlineCache onlineCache = new OnlineCache();
        byte[] onlineSessionsBegin = join(smBytes, ONLINE_STR_BYTES, packLong(0));
        byte[] onlineSessionEnd = join(smBytes, ONLINE_STR_BYTES, packLong(-1));
        Iterator<Entry<byte[], byte[]>> sessionIt = txn.getRange(onlineSessionsBegin, onlineSessionEnd);
        // For each online ID
        while(sessionIt.hasNext()) {
            Entry<byte[], byte[]> sessionEntry = sessionIt.next();
            if(sessionEntry.getKey().length != (smBytes.length + ONLINE_STR_BYTES.length + 8)) {
                continue;
            }
            byte[] sessionBytes = sessionEntry.getKey();
            long onlineID = unpackLong(sessionBytes, sessionBytes.length - 8);
            byte[] genBytes = txn.get(join(sessionBytes, GENERATION_STR_BYTES));
            Long generation = (genBytes == null) ? null : unpackLong(genBytes);

            // Load AISs
            byte[] protobufBytes = join(sessionBytes, PROTOBUF_STR_BYTES);
            Iterator<Entry<byte[], byte[]>> protobufIt = txn.getRange(join(protobufBytes, BYTES_00),
                                                                      join(protobufBytes, BYTES_FF));
            while(protobufIt.hasNext()) {
                Entry<byte[], byte[]> protobufEntry = protobufIt.next();
                byte[] schemaKey = protobufEntry.getKey();
                String schema = new String(schemaKey, protobufBytes.length, schemaKey.length - protobufBytes.length);
                Long prev = onlineCache.schemaToOnline.put(schema, onlineID);
                assert (prev == null) : String.format("%s, %d, %d", schema, prev, onlineID);
            }

            ProtobufReader reader = newProtobufReader();
            loadProtobufChildren(txn, reader, protobufBytes, Collections.<String>emptyList());
            loadProtobufChildren(txn, reader, join(smBytes, PROTOBUF_STR_BYTES), onlineCache.schemaToOnline.keySet());
            // Reader will have two copies of affected schemas, skip second (i.e. non-online)
            AkibanInformationSchema newAIS = finishReader(reader);
            validateAndFreeze(txn, newAIS, generation);
            buildRowDefs(newAIS);
            onlineCache.onlineToAIS.put(onlineID, newAIS);

            // Load ChangeSets
            byte[] changesBytes = join(sessionBytes, CHANGES_STR_BYTES);
            Iterator<Entry<byte[], byte[]>> changesIt = txn.getRange(join(changesBytes, packLong(0)),
                                                                     join(changesBytes, packLong(-1)));
            while(changesIt.hasNext()) {
                Entry<byte[], byte[]> changeEntry = changesIt.next();
                ChangeSet cs = ChangeSetHelper.load(changeEntry.getValue());
                Long prev = onlineCache.tableToOnline.put(cs.getTableId(), onlineID);
                assert (prev == null) : String.format("%d, %d, %d", cs.getTableId(), prev, onlineID);
                onlineCache.onlineToChangeSets.put(onlineID, cs);
            }
        }
        return onlineCache;
    }

    @Override
    protected void newTableVersions(Session session, Map<Integer, Integer> versions) {
        // Could track for checking concurrent table conflicts?
    }

    //
    // SchemaManager
    //

    @Override
    public void addOnlineHandledHKey(Session session, int tableID, Key hKey) {
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        if(onlineID == null) {
            throw new IllegalArgumentException("No online change for table: " + tableID);
        }
        MemoryTransaction txn = txnService.getTransaction(session);
        byte[] key = join(smBytes,
                          ONLINE_STR_BYTES,
                          packLong(onlineID),
                          HKEYS_STR_BYTES,
                          packLong(tableID),
                          Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize()));
        txn.set(key, BYTES_EMPTY);
    }

    @Override
    public Iterator<byte[]> getOnlineHandledHKeyIterator(Session session, int tableID, Key hKey) {
        OnlineSession onlineSession = getOnlineSession(session, true);
        MemoryTransaction txn = txnService.getTransaction(session);
        byte[] hkeyPrefix = join(smBytes,
                                 ONLINE_STR_BYTES,
                                 packLong(onlineSession.id),
                                 HKEYS_STR_BYTES,
                                 packLong(tableID));
        byte[] begin;
        if(hKey != null) {
            begin = join(hkeyPrefix, Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize()));
        } else {
            begin = join(hkeyPrefix, BYTES_00);
        }
        byte[] end = join(hkeyPrefix, BYTES_FF);
        final Iterator<Entry<byte[], byte[]>> iterator = txn.getRange(begin, end);
        final int prefixLength = hkeyPrefix.length;
        return new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
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

    @Override
    public void setOnlineDMLError(Session session, int tableID, String message) {
        MemoryTransaction txn = getTransaction(session);
        AkibanInformationSchema ais = getAis(session);
        OnlineCache onlineCache = getOnlineCache(session, ais);
        Long onlineID = onlineCache.tableToOnline.get(tableID);
        if(onlineID == null) {
            throw new IllegalArgumentException("No online change for table: " + tableID);
        }
        byte[] key = join(smBytes, ONLINE_STR_BYTES, packLong(onlineID), ERROR_STR_BYTES);
        txn.set(key, message.getBytes(UTF8));
    }

    @Override
    public String getOnlineDMLError(Session session) {
        MemoryTransaction txn = getTransaction(session);
        OnlineSession onlineSession = getOnlineSession(session, true);
        byte[] key = join(smBytes, ONLINE_STR_BYTES, packLong(onlineSession.id), ERROR_STR_BYTES);
        byte[] value = txn.get(key);
        return (value == null) ? null : new String(value, UTF8);
    }

    @Override
    public void addOnlineChangeSet(Session session, ChangeSet changeSet) {
        MemoryTransaction txn = getTransaction(session);
        OnlineSession onlineSession = getOnlineSession(session, true);
        onlineSession.tableIDs.add(changeSet.getTableId());

        byte[] sessionBytes = join(smBytes, ONLINE_STR_BYTES, packLong(onlineSession.id));
        txn.set(sessionBytes, BYTES_EMPTY);
        byte[] key = join(sessionBytes, CHANGES_STR_BYTES, packLong(changeSet.getTableId()));
        byte[] value = ChangeSetHelper.save(changeSet);
        txn.set(key, value);
        // TODO: Cleanup into Abstract. For consistency with PSSM.
        if(getAis(session).getGeneration() == getOnlineAIS(session).getGeneration()) {
            bumpGeneration(session);
        }
    }

    @Override
    public Set<String> getTreeNames(final Session session) {
        return txnService.run(session, new Callable<Set<String>>() {
            @Override
            public Set<String> call() throws Exception {
                AkibanInformationSchema ais = getAis(session);
                StorageNameVisitor visitor = new StorageNameVisitor();
                ais.visit(visitor);
                for(Sequence s : ais.getSequences().values()) {
                    visitor.visit(s);
                }
                visitor.names.add(unpackUUID(smBytes).toString());
                visitor.names.add(unpackUUID(tableStatusCache.getStatusPrefix()).toString());
                return visitor.names;
            }
        });
    }

    @Override
    public long getOldestActiveAISGeneration() {
        return (curAIS != null) ? curAIS.getGeneration() : -1;
    }

    @Override
    public Set<Long> getActiveAISGenerations() {
        return Collections.singleton(getOldestActiveAISGeneration());
    }

    @Override
    public boolean hasTableChanged(Session session, int tableID) {
        // Handled by locking
        return false;
    }

    //
    // Internal
    //

    private void attachToSession(Session session, AkibanInformationSchema ais) {
        // attachToSession
        AkibanInformationSchema prev = session.put(SESSION_AIS_KEY, ais);
        if(prev == null) {
            txnService.addCallback(session, TransactionService.CallbackType.END, CLEAR_SESSION_AIS);
        }
    }

    private void buildRowDefs(AkibanInformationSchema ais) {
        tableStatusCache.detachAIS();
        for(final Table table : ais.getTables().values()) {
            final TableStatus status;
            if(table.isVirtual()) {
                status = tableStatusCache.getOrCreateVirtualTableStatus(table.getTableId(),
                                                                        VirtualAdapter.getFactory(table));
            } else {
                status = tableStatusCache.createTableStatus(table);
            }
            table.tableStatus(status);
        }
        RowDefBuilder rowDefBuilder = new RowDefBuilder(null/*session*/, ais, tableStatusCache);
        rowDefBuilder.build();
    }

    private long getCurrentAISGeneration(MemoryTransaction txn) {
        byte[] key = join(smBytes, GENERATION_STR_BYTES);
        byte[] value = txn.get(key);
        return (value == null) ? 0 : unpackLong(value);
    }

    private long getNextAISGeneration(MemoryTransaction txn) {
        byte[] key = join(smBytes, GENERATION_STR_BYTES);
        byte[] value = txn.get(key);
        long nextVal = 1;
        if(value != null) {
            nextVal += unpackLong(value);
        }
        txn.set(key, packLong(nextVal));
        return nextVal;
    }

    private MemoryTransaction getTransaction(Session session) {
        return txnService.getTransaction(session);
    }

    private AkibanInformationSchema loadFromStorage(MemoryTransaction txn) {
        ProtobufReader reader = newProtobufReader();
        loadProtobufChildren(txn, reader, join(smBytes, PROTOBUF_STR_BYTES), Collections.<String>emptyList());
        finishReader(reader);
        validateAndFreeze(txn, reader.getAIS(), getCurrentAISGeneration(txn));
        AkibanInformationSchema ais = reader.getAIS();
        buildRowDefs(ais);
        return ais;
    }

    private void loadProtobufChildren(MemoryTransaction txn,
                                      ProtobufReader reader,
                                      byte[] prefix,
                                      Collection<String> withoutSchemas) {
        Iterator<Entry<byte[], byte[]>> protobufIt = txn.getRange(join(prefix, BYTES_00), join(prefix, BYTES_FF));
        while(protobufIt.hasNext()) {
            Entry<byte[], byte[]> entry = protobufIt.next();
            String schema = new String(entry.getKey(), prefix.length, entry.getKey().length - prefix.length);
            if(withoutSchemas.contains(schema)) {
                continue;
            }
            reader.loadBuffer(ByteBuffer.wrap(entry.getValue()));
        }
    }

    private ProtobufReader newProtobufReader() {
        return new ProtobufReader(typesRegistryService.getTypesRegistry(), storageFormatRegistry);
    }

    private void storeProtobuf(MemoryTransaction txn, byte[] keyPrefix, AkibanInformationSchema ais, String schema) {
        ProtobufWriter writer = new ProtobufWriter(new ProtobufWriter.SingleSchemaSelector(schema));
        writer.save(ais);
        ByteBuffer bb = ByteBuffer.allocate(writer.getBufferSize());
        writer.serialize(bb);
        txn.set(join(keyPrefix, schema.getBytes(UTF8)), bb.array());
    }

    private void validateAndFreeze(MemoryTransaction txn, AkibanInformationSchema ais, Long generation) {
        ais.validate(AISValidations.ALL_VALIDATIONS).throwIfNecessary();
        if(generation == null) {
            generation = getNextAISGeneration(txn);
        }
        ais.setGeneration(generation);
        ais.freeze();
    }

    //
    // Static
    //

    private static final Callback CLEAR_SESSION_AIS = new Callback()
    {
        @Override
        public void run(Session session, long timestamp) {
            session.remove(SESSION_AIS_KEY);
        }
    };

    private static class StorageNameVisitor extends AbstractVisitor
    {
        public final Set<String> names = new TreeSet<>();

        public void add(HasStorage hasStorage) {
            StorageDescription sd = hasStorage.getStorageDescription();
            if(sd instanceof MemoryStorageDescription) {
                MemoryStorageDescription tmsd = (MemoryStorageDescription)sd;
                names.add(tmsd.getNameString());
            }
        }

        @Override
        public void visit(Group group) {
            add(group);
        }

        @Override
        public void visit(Index index) {
            add(index);
        }

        public void visit(Sequence sequence) {
            add(sequence);
        }
    }

    private static AkibanInformationSchema finishReader(ProtobufReader reader) {
        reader.loadAIS();
        for(Table table : reader.getAIS().getTables().values()) {
            // nameGenerator is only needed to generate hidden PK, which shouldn't happen here
            table.endTable(null);
        }
        return reader.getAIS();
    }
}
