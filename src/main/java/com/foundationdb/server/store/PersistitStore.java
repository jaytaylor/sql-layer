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

import com.foundationdb.ais.model.*;
import com.foundationdb.ais.util.TableChangeValidator.ChangeLevel;
import com.foundationdb.qp.storeadapter.PersistitAdapter;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRow;
import com.foundationdb.qp.storeadapter.indexrow.PersistitIndexRowBuffer;
import com.foundationdb.qp.rowtype.IndexRowType;
import com.foundationdb.qp.rowtype.Schema;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.*;
import com.foundationdb.server.AccumulatorAdapter.AccumInfo;
import com.foundationdb.server.collation.CString;
import com.foundationdb.server.collation.CStringKeyCoder;
import com.foundationdb.server.error.*;
import com.foundationdb.server.error.DuplicateKeyException;
import com.foundationdb.server.rowdata.*;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.TreeService;
import com.foundationdb.server.store.TableChanges.ChangeSet;
import com.foundationdb.server.store.format.PersistitStorageDescription;
import com.foundationdb.server.store.format.protobuf.PersistitProtobufRow;
import com.foundationdb.server.store.format.protobuf.PersistitProtobufValueCoder;
import com.google.inject.Inject;
import com.persistit.*;
import com.persistit.encoding.CoderManager;
import com.persistit.encoding.ValueCoder;
import com.persistit.exception.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.persistit.Key.EQ;

public class PersistitStore extends AbstractStore<PersistitStore,Exchange,PersistitStorageDescription> implements Service
{
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStore.class);

    private static final String WRITE_LOCK_ENABLED_CONFIG = "fdbsql.write_lock_enabled";

    private boolean writeLockEnabled;

    private final ConfigurationService config;
    private final TreeService treeService;

    private RowDataValueCoder rowDataValueCoder;
    private PersistitProtobufValueCoder protobufValueCoder;

    @Inject
    public PersistitStore(TreeService treeService,
                          ConfigurationService config,
                          SchemaManager schemaManager,
                          ListenerService listenerService) {
        super(schemaManager, listenerService);
        if(!(schemaManager instanceof PersistitStoreSchemaManager)) {
            throw new IllegalArgumentException("PersistitStoreSchemaManager required, found: " + schemaManager.getClass());
        }
        this.treeService = treeService;
        this.config = config;
    }

    @Override
    public synchronized void start() {
        CoderManager cm = getDb().getCoderManager();
        cm.registerKeyCoder(CString.class, new CStringKeyCoder());
        cm.registerValueCoder(RowData.class, rowDataValueCoder = new RowDataValueCoder());
        cm.registerValueCoder(PersistitProtobufRow.class, protobufValueCoder = new PersistitProtobufValueCoder(this));
        if (config != null) {
            writeLockEnabled = Boolean.parseBoolean(config.getProperty(WRITE_LOCK_ENABLED_CONFIG));
        }
    }

    @Override
    public synchronized void stop() {
        getDb().getCoderManager().unregisterValueCoder(RowData.class);
        getDb().getCoderManager().unregisterKeyCoder(CString.class);
    }

    @Override
    public void crash() {
        stop();
    }

    @Override
    public Key createKey() {
        return treeService.createKey();
    }

    public Persistit getDb() {
        return treeService.getDb();
    }

    public Exchange getExchange(Session session, Group group) {
        return createStoreData(session, group);
    }

    public Exchange getExchange(final Session session, final RowDef rowDef) {
        return createStoreData(session, rowDef.getGroup());
    }

    public Exchange getExchange(final Session session, final Index index) {
        return createStoreData(session, index);
    }

    public void releaseExchange(final Session session, final Exchange exchange) {
        releaseStoreData(session, exchange);
    }

    private void constructIndexRow(Session session,
                                   Exchange exchange,
                                   RowData rowData,
                                   Index index,
                                   Key hKey,
                                   PersistitIndexRowBuffer indexRow,
                                   boolean forInsert) throws PersistitException
    {
        indexRow.resetForWrite(index, exchange.getKey(), exchange.getValue());
        indexRow.initialize(rowData, hKey);
        indexRow.close(session, this, forInsert);
    }

    public RowDataValueCoder getRowDataValueCoder() {
        return rowDataValueCoder;
    }
    public PersistitProtobufValueCoder getProtobufValueCoder() {
        return protobufValueCoder;
    }

    @Override
    protected Exchange createStoreData(Session session, PersistitStorageDescription storageDescription) {
        return treeService.getExchange(session, storageDescription);
    }

    @Override
    protected void releaseStoreData(Session session, Exchange exchange) {
        treeService.releaseExchange(session, exchange);
    }

    @Override
    PersistitStorageDescription getStorageDescription(Exchange exchange) {
        return (PersistitStorageDescription)exchange.getAppCache();
    }

    @Override
    public PersistitIndexRowBuffer readIndexRow(Session session,
                                                Index parentPKIndex,
                                                Exchange exchange,
                                                RowDef childRowDef,
                                                RowData childRowData)
    {
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(exchange.getKey());
        int[] fields = childRowDef.getParentJoinFields();
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            FieldDef fieldDef = childRowDef.getFieldDef(fields[fieldIndex]);
            keyAppender.append(fieldDef, childRowData);
        }
        try {
            exchange.fetch();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
        PersistitIndexRowBuffer indexRow = null;
        if (exchange.getValue().isDefined()) {
            indexRow = new PersistitIndexRowBuffer(this);
            indexRow.resetForRead(parentPKIndex, exchange.getKey(), exchange.getValue());
        }
        return indexRow;
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            truncateTree(session, index);
            if(index.isGroupIndex()) {
                try {
                    Tree tree = ((PersistitStorageDescription)index.getStorageDescription()).getTreeCache().getTree();
                    new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree).set(0);
                } catch(PersistitException | RollbackException e) {
                    throw PersistitAdapter.wrapPersistitException(session, e);
                }
            }
        }
    }

    private void checkNotGroupIndex(Index index) {
        if (index.isGroupIndex()) {
            throw new UnsupportedOperationException("can't update group indexes from PersistitStore: " + index);
        }
    }

    @Override
    protected void writeIndexRow(Session session,
                                 Index index,
                                 RowData rowData,
                                 Key hKey,
                                 PersistitIndexRowBuffer indexRow)
    {
        checkNotGroupIndex(index);
        Exchange iEx = getExchange(session, index);
        try {
            constructIndexRow(session, iEx, rowData, index, hKey, indexRow, true);
            checkUniqueness(session, rowData, index, iEx);
            iEx.store();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    private void checkUniqueness(Session session, RowData rowData, Index index, Exchange iEx) throws PersistitException
    {
        if (index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            Key key = iEx.getKey();
            int segmentCount = index.getKeyColumns().size();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            if (keyExistsInIndex(index, iEx)) {
                LOG.debug("Duplicate key for index {}, raw: {}", index.getIndexName(), key);
                String msg = formatIndexRowString(session, rowData, index);
                throw new DuplicateKeyException(index.getIndexName(), msg);
            }
        }
    }

    private boolean keyExistsInIndex(Index index, Exchange exchange) throws PersistitException
    {
        boolean keyExistsInIndex;
        // Passing -1 as the last argument of traverse leaves the exchange's key and value unmodified.
        // (0 would leave just the value unmodified.)
        if (index.isUnique()) {
            // The Persistit Key stores exactly the index key, so just check whether the key exists.
            // TODO:
            // The right thing to do is traverse(EQ, false, -1) but that returns true, even when the
            // tree is empty. Peter says this is a bug (1023549)
            keyExistsInIndex = exchange.traverse(Key.Direction.EQ, true, -1);
        } else {
            // Check for children by traversing forward from the current key.
            keyExistsInIndex = exchange.traverse(Key.Direction.GTEQ, true, -1);
        }
        return keyExistsInIndex;
    }

    private void deleteIndexRow(Session session,
                                Index index,
                                Exchange exchange,
                                RowData rowData,
                                Key hKey,
                                PersistitIndexRowBuffer indexRowBuffer)
        throws PersistitException
    {
        // Non-unique index: The exchange's key has all fields of the index row. If there is such a row it will be
        //     deleted, if not, exchange.remove() does nothing.
        // PK index: The exchange's key has the key fields of the index row, and a null separator of 0. If there is
        //     such a row it will be deleted, if not, exchange.remove() does nothing. Because PK columns are NOT NULL,
        //     the null separator's value must be 0.
        // Unique index with no nulls: Like the PK case.
        // Unique index with nulls: isUniqueAndMayContainNulls is true. The exchange's key is written with the
        //     key of the index row. There may be duplicates due to nulls, and they will have different null separator
        //     values and the hkeys will differ. Look through these until the desired hkey is found, and delete that
        //     row. If the hkey is missing, then the row is already not present.
        boolean deleted = false;
        PersistitAdapter adapter = adapter(session);
        if (index.isUniqueAndMayContainNulls()) {
            // Can't use a PIRB, because we need to get the hkey. Need a PersistitIndexRow.
            IndexRowType indexRowType = adapter.schema().indexRowType(index);
            PersistitIndexRow indexRow = adapter.takeIndexRow(indexRowType);
            constructIndexRow(session, exchange, rowData, index, hKey, indexRow, false);
            Key.Direction direction = Key.Direction.GTEQ;
            while (exchange.traverse(direction, true)) {
                // Delicate: copyFromExchange() initializes the key returned by hKey
                indexRow.copyFrom(exchange);
                PersistitHKey rowHKey = (PersistitHKey)indexRow.hKey();
                if (rowHKey.key().compareTo(hKey) == 0) {
                    deleted = exchange.remove();
                    break;
                }
                direction = Key.Direction.GT;
            }
            adapter.returnIndexRow(indexRow);
        } else {
            constructIndexRow(session, exchange, rowData, index, hKey, indexRowBuffer, false);
            deleted = exchange.remove();
        }
        assert deleted : "Exchange remove on deleteIndexRow";
    }

    @Override
    protected void deleteIndexRow(Session session,
                                  Index index,
                                  RowData rowData,
                                  Key hKey,
                                  PersistitIndexRowBuffer indexRowBuffer) {
        checkNotGroupIndex(index);
        Exchange iEx = getExchange(session, index);
        try {
            deleteIndexRow(session, index, iEx, rowData, hKey, indexRowBuffer);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void store(Session session, Exchange ex) {
        try {
            ex.store();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected boolean fetch(Session session, Exchange ex) {
        try {
            // ex.isValueDefined() doesn't actually fetch the value
            // ex.fetch() + ex.getValue().isDefined() would give false negatives (i.e. stored key with no value)
            return ex.traverse(EQ, true, Integer.MAX_VALUE);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected boolean clear(Session session, Exchange ex) {
        try {
            return ex.remove();
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    void resetForWrite(Exchange ex, Index index, PersistitIndexRowBuffer indexRowBuffer) {
        indexRowBuffer.resetForWrite(index, ex.getKey(), ex.getValue());
    }

    @Override
    protected Iterator<Void> createDescendantIterator(final Session session, final Exchange ex) {
        final Key hKey = ex.getKey();
        final KeyFilter filter = new KeyFilter(hKey, hKey.getDepth() + 1, Integer.MAX_VALUE);
        return new Iterator<Void>() {
            private Boolean lastExNext = null;

            @Override
            public boolean hasNext() {
                if(lastExNext == null) {
                    next();
                }
                return lastExNext;
            }

            @Override
            public Void next() {
                if(lastExNext != null) {
                    lastExNext = null;
                } else {
                    try {
                        lastExNext = ex.next(filter);
                    } catch(PersistitException | RollbackException e) {
                        throw PersistitAdapter.wrapPersistitException(session, e);
                    }
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    protected void sumAddGICount(Session session, Exchange ex, GroupIndex index, int count) {
        AccumulatorAdapter.sumAdd(AccumulatorAdapter.AccumInfo.ROW_COUNT, ex, count);
    }

    @Override
    protected void preWrite(Session session, Exchange storeData, RowDef rowDef, RowData rowData) {
        try {
            lockKeys(adapter(session), rowDef, rowData, storeData);
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected Key getKey(Session session, Exchange exchange) {
        return exchange.getKey();
    }

    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
    }

    @Override
    public void traverse(Session session, Group group, TreeRecordVisitor visitor) {
        Exchange exchange = getExchange(session, group);
        try {
            exchange.clear().append(Key.BEFORE);
            visitor.initialize(session, this);
            while(exchange.next(true)) {
                RowData rowData = new RowData();
                expandRowData(exchange, rowData);
                visitor.visit(exchange.getKey(), rowData);
            }
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, exchange);
        }
    }

    @Override
    public <V extends IndexVisitor<Key,Value>> V traverse(Session session, Index index, V visitor, long scanTimeLimit, long sleepTime) {
        Transaction xact = null;
        long nextCommitTime = 0;
        if (scanTimeLimit >= 0) {
            xact = treeService.getTransaction(session);
            nextCommitTime = System.currentTimeMillis() + scanTimeLimit;
        }
        Exchange exchange = getExchange(session, index).append(Key.BEFORE);
        try {
            while (exchange.next(true)) {
                visitor.visit(exchange.getKey(), exchange.getValue());
                if ((scanTimeLimit >= 0) &&
                    (System.currentTimeMillis() >= nextCommitTime)) {
                    xact.commit();
                    xact.end();
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        }
                        catch (InterruptedException ex) {
                            throw new QueryCanceledException(session);
                        }
                    }
                    xact.begin();
                    nextCommitTime = System.currentTimeMillis() + scanTimeLimit;
                }
            }
        } catch(PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, exchange);
        }
        return visitor;
    }

    @Override
    public void finishOnlineChange(Session session, Collection<ChangeSet> changeSets) {
        // None
    }

    private PersistitAdapter adapter(Session session)
    {
        return createAdapter(session, SchemaCache.globalSchema(schemaManager.getAis(session)));
    }

    private void lockKeys(PersistitAdapter adapter, RowDef rowDef, RowData rowData, Exchange exchange)
        throws PersistitException
    {
        // Temporary fix for #1118871 and #1078331 
        // disable the  lock used to prevent write skew for some cases of data loading
        if (!writeLockEnabled) return;
        Table table = rowDef.table();
        // Make fieldDefs big enough to accommodate PK field defs and FK field defs
        FieldDef[] fieldDefs = new FieldDef[table.getColumnsIncludingInternal().size()];
        Key lockKey = adapter.newKey();
        PersistitKeyAppender lockKeyAppender = PersistitKeyAppender.create(lockKey);
        // Primary key
        List<Column> pkColumns = table.getPrimaryKeyIncludingInternal().getColumns();
        for (int c = 0; c < pkColumns.size(); c++) {
            fieldDefs[c] = rowDef.getFieldDef(c);
        }
        lockKey(rowData, table, fieldDefs, pkColumns.size(), lockKeyAppender, exchange);
        // Grouping foreign key
        Join parentJoin = table.getParentJoin();
        if (parentJoin != null) {
            List<JoinColumn> joinColumns = parentJoin.getJoinColumns();
            for (int c = 0; c < joinColumns.size(); c++) {
                fieldDefs[c] = rowDef.getFieldDef(joinColumns.get(c).getChild().getPosition());
            }
            lockKey(rowData, parentJoin.getParent(), fieldDefs, joinColumns.size(), lockKeyAppender, exchange);
        }
    }

    private void lockKey(RowData rowData,
                         Table lockTable,
                         FieldDef[] fieldDefs,
                         int nFields,
                         PersistitKeyAppender lockKeyAppender,
                         Exchange exchange)
        throws PersistitException
    {
        // Write ordinal id to the lock key
        lockKeyAppender.key().append(lockTable.getOrdinal());
        // Write column values to the lock key
        for (int f = 0; f < nFields; f++) {
            lockKeyAppender.append(fieldDefs[f], rowData);
        }
        exchange.lock(lockKeyAppender.key());
        lockKeyAppender.clear();
    }

    @Override
    public PersistitAdapter createAdapter(Session session, Schema schema) {
        return new PersistitAdapter(schema, this, treeService, session, config);
    }

    @Override
    public boolean treeExists(Session session, StorageDescription storageDescription) {
        return treeService.treeExists(storageDescription.getSchemaName(), ((PersistitStorageDescription)storageDescription).getTreeName());
    }

    @Override
    public boolean isRetryableException(Throwable t) {
        if (t instanceof PersistitAdapterException) {
            t = t.getCause();
        }
        return (t instanceof RollbackException);
    }

    @Override
    public long nullIndexSeparatorValue(Session session, Index index) {
        Tree tree = ((PersistitStorageDescription)index.getStorageDescription()).getTreeCache().getTree();
        AccumulatorAdapter accumulator = new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.UNIQUE_ID, tree);
        return accumulator.seqAllocate();
    }

    @Override
    public void truncateTree(Session session, HasStorage object) {
        Exchange iEx = treeService.getExchange(session, (PersistitStorageDescription)object.getStorageDescription());
        try {
            iEx.removeAll();
        } catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void removeTree(Session session, HasStorage object) {
        PersistitStorageDescription storageDescription = (PersistitStorageDescription)object.getStorageDescription();
        ((PersistitStoreSchemaManager)schemaManager).treeWasRemoved(session, object.getSchemaName(), storageDescription.getTreeName());
    }

    @Override
    public long nextSequenceValue(Session session, Sequence sequence) {
        // Note: Ever increasing, always incremented by 1, rollbacks will leave gaps. See bug1167045 for discussion.
        AccumulatorAdapter accum = getAdapter(sequence);
        long rawSequence = accum.seqAllocate();
        return sequence.realValueForRawNumber(rawSequence);
    }
    
    @Override 
    public long curSequenceValue(Session session, Sequence sequence) {
        AccumulatorAdapter accum = getAdapter(sequence);
        try {
            return sequence.realValueForRawNumber(accum.getSnapshot());
        } catch (PersistitException | RollbackException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    private AccumulatorAdapter getAdapter(Sequence sequence)  {
        Tree tree = ((PersistitStorageDescription)sequence.getStorageDescription()).getTreeCache().getTree();
        return new AccumulatorAdapter(AccumInfo.SEQUENCE, tree);
    }
    
    @Override
    public String getName() {
        return "Persistit " + Persistit.version();
    }
}
