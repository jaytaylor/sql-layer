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

import com.akiban.ais.model.*;
import com.akiban.ais.model.Index.IndexType;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRow;
import com.akiban.qp.persistitadapter.indexrow.PersistitIndexRowBuffer;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.server.*;
import com.akiban.server.collation.CString;
import com.akiban.server.collation.CStringKeyCoder;
import com.akiban.server.error.*;
import com.akiban.server.error.DuplicateKeyException;
import com.akiban.server.rowdata.*;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.lock.LockService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.text.FullTextIndexService;
import com.akiban.server.service.tree.TreeLink;
import com.akiban.server.service.tree.TreeService;
import com.google.inject.Inject;
import com.persistit.*;
import com.persistit.Management.DisplayFilter;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.persistit.Key.EQ;

public class PersistitStore extends AbstractStore<Exchange> implements Service
{
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStore.class);

    private static final String WRITE_LOCK_ENABLED_CONFIG = "akserver.write_lock_enabled";
    private static final String MAINTENANCE_SCHEMA = "maintenance";
    private static final String FULL_TEXT_TABLE = "full_text";

    private boolean writeLockEnabled;

    private final ConfigurationService config;

    private final TreeService treeService;

    private final SchemaManager schemaManager;

    private DisplayFilter originalDisplayFilter;

    private FullTextIndexService fullTextService;

    private RowDataValueCoder valueCoder;

    // Each row change has a 'uniqueChangeId', stored in the 'maintenance.full_text' table
    // The number would be reset after all maintenance is done
    private volatile AtomicLong uniqueChangeId = new AtomicLong(0);


    @Inject
    public PersistitStore(TreeService treeService,
                          ConfigurationService config,
                          SchemaManager schemaManager,
                          LockService lockService) {
        super(lockService, schemaManager);
        this.treeService = treeService;
        this.config = config;
        this.schemaManager = schemaManager;
    }


    //
    // FullText change tracking
    // TODO: Move out of PersistitStore
    //

    public void setFullTextService(FullTextIndexService service)
    {
        fullTextService = service;
    }

     // --- for tracking changes 
    private Exchange getChangeExchange(Session session) throws PersistitException
    {
        return treeService.getExchange(session,
                                       treeService.treeLink(MAINTENANCE_SCHEMA,
                                                            FULL_TEXT_TABLE));
    }

    private void addChange(Session session,
                           String schema,
                           String table,
                           String index,
                           Integer indexId,
                           Key rowHKey)
    {
        try {
            Exchange ex = getChangeExchange(session);

            // KEY: schema | table | indexName | indexId | unique_num
            ex.clear().append(schema).append(table).append(index).append(indexId).append(uniqueChangeId.getAndIncrement());

            // VALUE: rowHKey's bytes | indexId
            ex.getValue().clear().putByteArray(rowHKey.getEncodedBytes(),
                                               0,
                                               rowHKey.getEncodedSize());
            ex.store();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected void addChangeFor(Session session, UserTable table, Key hKey)
    {
        for (Index index : table.getFullTextIndexes())
        {
            IndexName idxName = index.getIndexName();
            addChange(session,
                      idxName.getSchemaName(),
                      idxName.getTableName(),
                      idxName.getName(),
                      index.getIndexId(),
                      hKey);
        }
    }
    // --- for reporting changes

    /**
     * Collect all 'HKeyRow's from the list of key-value pairs that have
     * the same schema.table.indexName,
     * also fill schema[0],
     *           table[0],
     *      and  indexName[0]
     *  with correct values
     */
    public HKeyBytesStream getChangedRows(Session session) throws PersistitException
    {
        Exchange ex = getChangeExchange(session);
        ex.append(Key.BEFORE);
        return new HKeyBytesStream(ex, session);
    }

 
    /**
     * 
     * This is supposed to behave similar* to Scheme's stream,
     * (ie., each element is not computed until it absolutely has to be.)
     * 
     * In other words, the byte arrays are not decoded before hand
     */
    public class HKeyBytesStream implements Iterable<byte[]>
    {
        private final Exchange ex;
        private KeyFilter filter;
        private IndexName indexName;
        private final Session session;

        // 'private' because this should not be constructed
        // anywhere other than PersistitStore.getChangedRows()
        // The class itself, however, is public, as it can be used anywhere
        private HKeyBytesStream (Exchange ex, Session session) throws PersistitException
        {
            this.ex = ex;
            this.session = session;
            findNextIndex();
        }

        private void findNextIndex() throws PersistitException
        {
            indexName = null;
            Key key = ex.getKey();
            while(ex.next(true)) {
                indexName = new IndexName(new TableName(key.decodeString(), key.decodeString()), key.decodeString());
                int indexID = key.decodeInt();
                UserTable table = getAIS(session).getUserTable(indexName.getFullTableName());
                Index index = (table != null) ? table.getFullTextIndex(indexName.getName()) : null;
                // May have been deleted or recreated
                if(index != null && index.getIndexId() == indexID) {
                    key.cut(); // Remove unique id
                    filter = new KeyFilter(ex.getKey());
                    break;
                }
                ex.remove();
                indexName = null;
            }
        }

        public boolean hasStream()
        {
            return indexName != null;
        }

        public IndexName getIndexName()
        {
            return indexName;
        }

        @Override
        public Iterator<byte[]> iterator()
        {
            return new StreamIterator(hasStream());
        }

        private class StreamIterator implements Iterator<byte[]>
        {
            private Boolean hasNext;

            private StreamIterator(boolean hasNext) {
                this.hasNext = hasNext;
            }

            private void advance()
            {
                try {
                    hasNext = ex.next(filter);
                } catch(PersistitException e) {
                    throw PersistitAdapter.wrapPersistitException(session, e);
                }
            }

            @Override
            public boolean hasNext()
            {
                if(hasNext == null) {
                    advance();
                }
                return hasNext;
            }

            @Override
            public byte[] next()
            {
                if(hasNext == null) {
                    advance();
                }
                if(!hasNext) {
                    throw new NoSuchElementException();
                }
                hasNext = null;
                return ex.getValue().getByteArray();
            }

            @Override
            public void remove()
            {
                try {
                    ex.remove();
                } catch(PersistitException e) {
                    throw PersistitAdapter.wrapPersistitException(session, e);
                }
            }   
        }
    }

    //------ end fulltext index maintenance services ------

    @Override
    public synchronized void start() {
        try {
            CoderManager cm = getDb().getCoderManager();
            Management m = getDb().getManagement();
            cm.registerValueCoder(RowData.class, valueCoder = new RowDataValueCoder());
            cm.registerKeyCoder(CString.class, new CStringKeyCoder());
            originalDisplayFilter = m.getDisplayFilter();
            m.setDisplayFilter(new RowDataDisplayFilter(originalDisplayFilter));
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
        if (config != null) {
            writeLockEnabled = Boolean.parseBoolean(config.getProperty(WRITE_LOCK_ENABLED_CONFIG));
        }
    }

    @Override
    public synchronized void stop() {
        try {
            getDb().getCoderManager().unregisterValueCoder(RowData.class);
            getDb().getCoderManager().unregisterKeyCoder(CString.class);
            getDb().getManagement().setDisplayFilter(originalDisplayFilter);
        } catch (RemoteException e) {
            throw new DisplayFilterSetException (e.getMessage());
        }
    }

    @Override
    public void crash() {
        stop();
    }

    @Override
    public Key createKey() {
        return treeService.createKey();
    }

    @Override
    public PersistitStore getPersistitStore() {
        return this;
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
        return createStoreData(session, index.indexDef());
    }

    public void releaseExchange(final Session session, final Exchange exchange) {
        releaseStoreData(session, exchange);
    }

    private static void constructIndexRow(Exchange exchange,
                                          RowData rowData,
                                          Index index,
                                          Key hKey,
                                          PersistitIndexRowBuffer indexRow,
                                          boolean forInsert) throws PersistitException
    {
        indexRow.resetForWrite(index, exchange.getKey(), exchange.getValue());
        indexRow.initialize(rowData, hKey);
        indexRow.close(forInsert);
    }

    @Override
    protected Exchange createStoreData(Session session, TreeLink treeLink) {
        return treeService.getExchange(session, treeLink);
    }

    @Override
    protected void releaseStoreData(Session session, Exchange exchange) {
        treeService.releaseExchange(session, exchange);
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
        } catch(PersistitException e) {
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
    public void truncateGroup(final Session session, final Group group) {
        List<Index> indexes = new ArrayList<>();
        // Truncate the group tree
        final Exchange hEx = getExchange(session, group);
        try {
            // Collect indexes, truncate table statuses
            for(UserTable table : group.getRoot().getAIS().getUserTables().values()) {
                if(table.getGroup() == group) {
                    indexes.addAll(table.getIndexesIncludingInternal());
                    table.rowDef().getTableStatus().truncate(session);
                }
            }
            indexes.addAll(group.getIndexes());
            truncateIndexes(session, indexes);

            hEx.removeAll();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, hEx);
        }
    }

    @Override
    public void truncateIndexes(Session session, Collection<? extends Index> indexes) {
        super.truncateIndexes(session, indexes);
        for(Index index : indexes) {
            if(index.isGroupIndex()) {
                try {
                    Tree tree = index.indexDef().getTreeCache().getTree();
                    new AccumulatorAdapter(AccumulatorAdapter.AccumInfo.ROW_COUNT, tree).set(0);
                } catch(PersistitInterruptedException e) {
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
            constructIndexRow(iEx, rowData, index, hKey, indexRow, true);
            checkUniqueness(index, rowData, iEx);
            iEx.store();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    private void checkUniqueness(Index index, RowData rowData, Exchange iEx) throws PersistitException
    {
        if (index.isUnique() && !hasNullIndexSegments(rowData, index)) {
            Key key = iEx.getKey();
            int segmentCount = index.indexDef().getIndexKeySegmentCount();
            // An index that isUniqueAndMayContainNulls has the extra null-separating field.
            if (index.isUniqueAndMayContainNulls()) {
                segmentCount++;
            }
            key.setDepth(segmentCount);
            if (keyExistsInIndex(index, iEx)) {
                throw new DuplicateKeyException(index.getIndexName().getName(), key);
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
            constructIndexRow(exchange, rowData, index, hKey, indexRow, false);
            Key.Direction direction = Key.Direction.GTEQ;
            while (exchange.traverse(direction, true)) {
                indexRow.copyFromExchange(exchange); // Gets the current state of the exchange into oldIndexRow
                PersistitHKey rowHKey = (PersistitHKey) indexRow.hKey();
                if (rowHKey.key().compareTo(hKey) == 0) {
                    deleted = exchange.remove();
                    break;
                }
                direction = Key.Direction.GT;
            }
            adapter.returnIndexRow(indexRow);
        } else {
            constructIndexRow(exchange, rowData, index, hKey, indexRowBuffer, false);
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
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void packRowData(Exchange ex, RowData rowData) {
        Value value = ex.getValue();
        value.directPut(valueCoder, rowData, null);
    }

    @Override
    public void store(Session session, Exchange ex) {
        try {
            ex.store();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected boolean fetch(Session session, Exchange ex) {
        try {
            // ex.isValueDefined() doesn't actually fetch the value
            // ex.fetch() + ex.getValue().isDefined() would give false negatives (i.e. stored key with no value)
            return ex.traverse(EQ, true, Integer.MAX_VALUE);
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected boolean clear(Session session, Exchange ex) {
        try {
            return ex.remove();
        } catch(PersistitException e) {
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
                    } catch(PersistitException e) {
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
    public void expandRowData(final Exchange exchange, final RowData rowData) {
        final Value value = exchange.getValue();
        try {
            value.directGet(valueCoder, rowData, RowData.class, null);
        }
        catch(CorruptRowDataException e) {
            LOG.error("Corrupt RowData at key {}: {}", exchange.getKey(), e.getMessage());
            throw new RowDataCorruptionException(exchange.getKey());
        }
        // UNNECESSARY: Already done by value.directGet(...)
        // rowData.prepareRow(0);
    }

    @Override
    public void buildIndexes(Session session, Collection<? extends Index> indexes) {
        // TODO: Generalize. Knowing about FullTextService is wrong.
        Collection<Index> nonFTIndexes = new ArrayList<>();
        for(Index index : indexes) {
            if(index.getIndexType() == IndexType.FULL_TEXT) {
                // This schedules a deferred process to populate the
                // full text index at a later date (starting in a few seconds).
                fullTextService.schedulePopulate(session, index.getIndexName());
            } else {
                nonFTIndexes.add(index);
            }
        }
        super.buildIndexes(session, nonFTIndexes);
    }

    @Override
    public void removeTrees(Session session, UserTable table) {
        super.removeTrees(session, table);

        // TODO: Generalize. Knowing about FullTextService is wrong.
        for(FullTextIndex idx : table.getOwnFullTextIndexes()) {
            fullTextService.dropIndex(session, idx);
        }
    }

    @Override
    protected void preWrite(Session session, Exchange storeData, RowDef rowDef, RowData rowData) {
        try {
            lockKeys(adapter(session), rowDef, rowData, storeData);
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }

    @Override
    protected Key getKey(Session session, Exchange exchange) {
        return exchange.getKey();
    }

    @Override
    public void deleteIndexes(final Session session, final Collection<? extends Index> indexes) {
        super.deleteIndexes(session, indexes);
        // TODO: Generalize. Knowing about FullTextService is wrong.
        for(Index index : indexes) {
            // no trees to drop
            if (index.getIndexType() == IndexType.FULL_TEXT)
            {
                fullTextService.dropIndex(session, (FullTextIndex) index);
            }
        }
    }
    
    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
    }

    public void traverse(Session session, Group group, TreeRecordVisitor visitor) throws PersistitException {
        Exchange exchange = getExchange(session, group);
        try {
            exchange.clear().append(Key.BEFORE);
            visitor.initialize(session, this, exchange);
            while (exchange.next(true)) {
                visitor.visit();
            }
        } finally {
            releaseExchange(session, exchange);
        }
    }

    @Override
    public <V extends IndexVisitor<Key,Value>> V traverse(Session session, Index index, V visitor) {
        Exchange exchange = getExchange(session, index).append(Key.BEFORE);
        try {
            while (exchange.next(true)) {
                visitor.visit(exchange.getKey(), exchange.getValue());
            }
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, exchange);
        }
        return visitor;
    }

    private static PersistitAdapter adapter(Session session)
    {
        return (PersistitAdapter) session.get(StoreAdapter.STORE_ADAPTER_KEY);
    }

    private void lockKeys(PersistitAdapter adapter, RowDef rowDef, RowData rowData, Exchange exchange)
        throws PersistitException
    {
        // Temporary fix for #1118871 and #1078331 
        // disable the  lock used to prevent write skew for some cases of data loading
        if (!writeLockEnabled) return;
        UserTable table = rowDef.userTable();
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
                         UserTable lockTable,
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
    public void truncateTree(Session session, TreeLink treeLink) {
        Exchange iEx = treeService.getExchange(session, treeLink);
        try {
            iEx.removeAll();
        } catch (PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            releaseExchange(session, iEx);
        }
    }

    @Override
    public void removeTree(Session session, TreeLink treeLink) {
        try {
            if(!schemaManager.treeRemovalIsDelayed()) {
                Exchange ex = treeService.getExchange(session, treeLink);
                ex.removeTree();
                // Do not releaseExchange, causes caching and leak for now unused tree
            }
            schemaManager.treeWasRemoved(session, treeLink.getSchemaName(), treeLink.getTreeName());
        } catch (PersistitException e) {
            LOG.debug("Exception removing tree from Persistit", e);
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
    }
}
