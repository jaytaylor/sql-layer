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
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.api.dml.scan.LegacyRowWrapper;
import com.akiban.server.api.dml.scan.NewRow;
import com.akiban.server.api.dml.scan.NiceRow;
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
import com.akiban.util.tap.InOutTap;
import com.akiban.util.tap.Tap;
import com.persistit.*;
import com.persistit.Management.DisplayFilter;
import com.persistit.encoding.CoderManager;
import com.persistit.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class PersistitStore extends AbstractStore<Exchange> implements Service
{
    private static final Logger LOG = LoggerFactory.getLogger(PersistitStore.class);

    private static final InOutTap UPDATE_ROW_TAP = Tap.createTimer("write: update_row");

    private static final InOutTap DELETE_ROW_TAP = Tap.createTimer("write: delete_row");

    private static final InOutTap TABLE_INDEX_MAINTENANCE_TAP = Tap.createTimer("index: maintain_table");

    private static final String WRITE_LOCK_ENABLED_CONFIG = "akserver.write_lock_enabled";
    private static final String MAINTENANCE_SCHEMA = "maintenance";
    private static final String FULL_TEXT_TABLE = "full_text";

    private final static byte[] EMPTY_BYTE_ARRAY = new byte[0];
    
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
        HKeyBytesStream ret;
        
        if (ex.hasNext(true)                                           // if the tree is not empty
                && (ret = new HKeyBytesStream(ex, session)).hasNext()) // and does not contain only invalid indices
        {
            return ret;
        }
        else
        {
            return null;
        }
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
        private IndexName indexName;
        private int modCount = 0;
        private boolean moreRows; // more rows of this index
        private final Session session;
        private boolean eot; // end of tree;
        private HKeyBytesStream()
        {
            ex = null;
            session = null;
        }

        // 'private' because this should not be constructed
        // anywhere other than PersistitStore.getChangedRows()
        // The class itself, however, is public, as it can be used anywhere
        private HKeyBytesStream (Exchange ex, Session session) throws PersistitException
        {
            this.ex = ex;
            this.session = session;
            eot = false;
            moreRows = true;
            nextIndex(); // at the beginning of the tree now. look for the first legit index
        }

        public final boolean nextIndex() throws PersistitException
        {
            if (eot)
                return false;

            boolean nextIndex = ex.next(true);
            if (nextIndex)
            {
                modCount = 0;
                indexName = buildName(ex);
                if (eot = !ignoreDeleted(ex, indexName, true)) // reach the end after ignoring all deleted indexId
                    return moreRows = false;                   // hence no more rows (or indices)
            }
            else
            {
                eot = true;
                moreRows = false;
            }

            return nextIndex;
        }

        public boolean hasNext()
        {
            return !eot;
        }

        public IndexName getIndexName()
        {
            return indexName;
        }

        @Override
        public Iterator<byte[]> iterator()
        {
            return new StreamIterator(moreRows);
        }

        /**
         * remove all change-entries 
         * 
         * 
         */
        public void removeAll() throws PersistitException
        {
            ex.removeAll(); 
            ++modCount;
        }

        private class StreamIterator implements Iterator<byte[]>
        {

            private boolean hasNext;
            private int innerModCount = modCount;

            private StreamIterator (boolean hasNext)
            {
                this.hasNext = hasNext;
            }

            @Override
            public boolean hasNext()
            {
                return hasNext;
            }

            @Override
            public byte[] next()
            {
                try
                {
                    if (innerModCount != modCount)
                        throw new ConcurrentModificationException();

                    byte ret[] = ex.getValue().getByteArray();
                    boolean seeNewIndex = false;
                    boolean hasMore;
                    hasNext = (hasMore = ex.next(true)) 
                                        && !(seeNewIndex = seeNewIndex(indexName.getSchemaName(),
                                                                       indexName.getTableName(),
                                                                       indexName.getName(),
                                                                       ex.getKey())
                                        // and following this row is at least one row of
                                        // this index whose index-id is valid
                                        && ignoreDeleted(ex, indexName, false));

                    // if there are no more entries,
                    // set eot so the 'outer loop', which takes care of each index
                    // does not attempt to do 'next(true)'
                    // (because surpringly, after the end has been reached
                    //  the key will be set to BEFORE, therefore next(true)
                    /// will return 'true'. ==> infinite loop!)
                    eot = !hasMore;

                    // Take caution in order NOT to go to the next index (ie., different name)
                    if (seeNewIndex)  
                        // back up one entry, because we have read past the last
                        // entry that has the same schema.table.indexName
                        ex.previous(); 
                    ++innerModCount;
                    ++modCount;

                    return ret;
                }
                catch (PersistitException ex)
                {
                    throw new AkibanInternalException("Error retrieving rows from Exchange", ex);
                }
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException("Not supported yet.");
            }   
        }

        /**
         * Skip all entries whose indexName is that of a non-existing index,
         * or whose indexId is not the same as that of the the current index (with the same name)
         * 
         * @param ex
         * @param indexName
         * @param lookPastNewIndex whether to go beyond the current index
         * @return <FALSE> iff after ignoring all deleted indices, there's nothing left
         *         <TRUE> otherwise
         * @throws PersistitException 
         */
        private boolean ignoreDeleted(Exchange ex,
                                      IndexName indexName,
                                      boolean lookPastNewIndex)
                        throws PersistitException
        {
            // KEY: Schema | Table | indexName | indexID | ...
            Key key = ex.getKey();
            key.reset();
            
            key.reset();
            assert indexName.getSchemaName().equals(key.decodeString()) 
                    : "Unexpected schema" ;
            assert indexName.getTableName().equals(key.decodeString())
                    : "Unexpected table";
            assert indexName.getName().equals(key.decodeString())
                    : "Unexpected name";

            Integer indexId = key.decodeInt();
            while (sameIdAsCurrent(session, indexName, indexId) != NOCHANGE)
            // there was a DELETE (and possibly RECREATE)
            // ignore all changes in the old id(s)
            {
                boolean hasMore = true;
                boolean seeNewIndex = false;

                do
                { /*do nothing (skipping deleted 'index')*/}
                while ((hasMore = ex.next(true))
                                 && !(seeNewIndex = seeNewIndex(indexName.getSchemaName(),
                                                                indexName.getTableName(),
                                                                indexName.getName(),
                                                                ex.getKey()))
                                 && !seeNewIndexId(ex, indexId));

                if (eot = !hasMore)  // reach the end! (no more rows or indices!)
                    return false;    // set 'eot' for the same reason in StreamIterator.next() 

                else if (seeNewIndex) // back up one entry in order not to 
                {                     // go past the last pair
                    ex.previous(true);
                    
                    // Saw new index,
                    // hence do the checking again,
                    // if we're allowed to look past new index
                    // (otherwise, meaning no more rows with good ids of this
                    //   index, return false)
                    return lookPastNewIndex ? nextIndex() : false;
                }
                else // ie., see new IndexId
                {
                    Key k = ex.getKey();
                    k.reset();
                    k.decodeString();
                    k.decodeString();
                    k.decodeString();

                    indexId = k.decodeInt();
                }
            }
            return true;
        }

        private boolean seeNewIndexId(Exchange ex, Integer oldId)
        {
            Key key = ex.getKey();
            key.reset();
            // skip uninteresting parts
            key.decodeString();
            key.decodeString();
            key.decodeString();

            return !oldId.equals(key.decodeInt());
            
        }

        /**
         * Check if the given id is the same as the current index's id.
         * 
         * (If the index with the given name no longer exists, return false)
         */
        private int sameIdAsCurrent(Session session, IndexName indexName, Integer id)
        {
            AkibanInformationSchema ais = getAIS(session);
            UserTable table = ais.getUserTable(indexName.getFullTableName());
            Index index;
            if (table == null || (index = table.getFullTextIndex(indexName.getName())) == null)
                return DELETED;
            
            return index.getIndexId() != id ? RECREATED : NOCHANGE;
        }

        private boolean seeNewIndex(String schema, String table, String indexName, Key k)
        {
            k.reset();
            return !k.decodeString().equals(schema)
                    || !k.decodeString().equals(table)
                    || !k.decodeString().equals(indexName);
        }

        private IndexName buildName(Exchange e)
        {
            Key key = e.getKey();
            key.reset();
            return new IndexName(new TableName(key.decodeString(),
                                               key.decodeString()),
                                 key.decodeString());
        }
        
        // status of index
        private static final int NOCHANGE = 0;
        private static final int DELETED = 1;
        private static final int RECREATED = 2;
        
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

    // Promote visibility. TODO: Remove when OperatorStore goes away.
    @Override
    public void constructHKey(Session session, RowDef rowDef, RowData rowData, boolean isInsert, Key hKeyOut) {
        super.constructHKey(session, rowDef, rowData, isInsert, hKeyOut);
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
                                                Index index,
                                                Exchange exchange,
                                                RowDef rowDef,
                                                RowData rowData)
    {
        PersistitKeyAppender keyAppender = PersistitKeyAppender.create(exchange.getKey());
        int[] fields = rowDef.getParentJoinFields();
        for (int fieldIndex = 0; fieldIndex < fields.length; fieldIndex++) {
            FieldDef fieldDef = rowDef.getFieldDef(fields[fieldIndex]);
            keyAppender.append(fieldDef, rowData);
        }
        try {
            exchange.fetch();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
        PersistitIndexRowBuffer indexRow = null;
        if (exchange.getValue().isDefined()) {
            indexRow = new PersistitIndexRowBuffer(this);
            indexRow.resetForRead(index, exchange.getKey(), exchange.getValue());
        }
        return indexRow;
    }

    // --------------------- Implement Store interface --------------------

    @Override
    public void deleteRow(Session session, RowData rowData, boolean deleteIndexes, boolean cascadeDelete)
    {
        deleteRow(session, rowData, deleteIndexes, cascadeDelete, null, true);
    }

    private void deleteRow(Session session, RowData rowData, boolean deleteIndexes, boolean cascadeDelete,
                           BitSet tablesRequiringHKeyMaintenance, boolean propagateHKeyChanges)
    {
        RowDef rowDef = writeCheck(session, rowData);
        Exchange hEx = null;
        
        DELETE_ROW_TAP.in();
        try {
            hEx = getExchange(session, rowDef);
            
            lockKeys(adapter(session), rowDef, rowData, hEx);
            constructHKey(session, rowDef, rowData, false, hEx.getKey());
            hEx.fetch();
            //
            // Verify that the row exists
            //
            if (!hEx.getValue().isDefined()) {
                throw new NoSuchRowException(hEx.getKey());
            }
            // record the deletion of the old index row
            if (deleteIndexes)
                addChangeFor(session, rowDef.userTable(), hEx.getKey());

            // Remove the h-row
            hEx.remove();
            rowDef.getTableStatus().rowDeleted(session);

            // Remove the indexes, including the PK index

            PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
            if(deleteIndexes) {
                for (Index index : rowDef.getIndexes()) {
                    deleteIndexRow(session, index, rowData, hEx.getKey(), indexRow);
                }
            }

            // The row being deleted might be the parent of rows that
            // now become orphans. The hkeys
            // of these rows need to be maintained.
            if(propagateHKeyChanges && rowDef.userTable().hasChildren()) {
                propagateDownGroup(session, hEx, tablesRequiringHKeyMaintenance, indexRow, deleteIndexes, cascadeDelete);
            }
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            DELETE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

    @Override
    public void updateRow(Session session,
                          RowData oldRowData,
                          RowData newRowData,
                          ColumnSelector columnSelector,
                          Index[] indexes)
    {
        updateRow(session, oldRowData, newRowData, columnSelector, indexes, (indexes != null), true);
    }

    private void updateRow(Session session,
                           RowData oldRowData,
                           RowData newRowData,
                           ColumnSelector columnSelector,
                           Index[] indexesToMaintain,
                           boolean indexesAsInsert,
                           boolean propagateHKeyChanges)
    {
        int rowDefId = oldRowData.getRowDefId();
        if (newRowData.getRowDefId() != rowDefId) {
            throw new IllegalArgumentException("RowData values have different rowDefId values: ("
                                                       + rowDefId + "," + newRowData.getRowDefId() + ")");
        }

        // RowDefs may be different (e.g. during an ALTER)
        // Only non-pk or grouping columns could have change in this scenario
        RowDef rowDef = writeCheck(session, oldRowData);
        RowDef newRowDef = rowDefFromExplicitOrId(session, newRowData);
        PersistitAdapter adapter = adapter(session);
        Exchange hEx = null;

        UPDATE_ROW_TAP.in();
        try {
            hEx = getExchange(session, rowDef);
            lockKeys(adapter, rowDef, oldRowData, hEx);
            lockKeys(adapter, newRowDef, newRowData, hEx);
            constructHKey(session, rowDef, oldRowData, false, hEx.getKey());
            hEx.fetch();
            //
            // Verify that the row exists
            //
            if (!hEx.getValue().isDefined()) {
                throw new NoSuchRowException (hEx.getKey());
            }

            // Combine current version of row with the version coming in
            // on the update request.
            // This is done by taking only the values of columns listed
            // in the column selector.
            RowData currentRow = new RowData(EMPTY_BYTE_ARRAY);
            expandRowData(hEx, currentRow);
            RowData mergedRowData = 
                columnSelector == null 
                ? newRowData
                : mergeRows(rowDef, currentRow, newRowData, columnSelector);
            BitSet tablesRequiringHKeyMaintenance =
                    propagateHKeyChanges
                    ? analyzeFieldChanges(session, rowDef, oldRowData, mergedRowData)
                    : null;
            if (tablesRequiringHKeyMaintenance == null) {
                // No PK or FK fields have changed. Just update the row.
                packRowData(session, hEx, mergedRowData);
                // Store the h-row
                hEx.store();
                // Update the indexes (new row)
                addChangeFor(session, newRowDef.userTable(), hEx.getKey());
                
                PersistitIndexRowBuffer indexRowBuffer = new PersistitIndexRowBuffer(this);
                Index[] indexes = (indexesToMaintain == null) ? rowDef.getIndexes() : indexesToMaintain;
                for (Index index : indexes) {
                    if(indexesAsInsert) {
                        writeIndexRow(session, index, mergedRowData, hEx.getKey(), indexRowBuffer);
                    } else {
                        updateIndex(session, index, rowDef, currentRow, mergedRowData, hEx.getKey(), indexRowBuffer);
                    }
                }
            } else {
                // A PK or FK field has changed. The row has to be deleted and reinserted, and hkeys of descendent
                // rows maintained. tablesRequiringHKeyMaintenance contains the ordinals of the tables whose hkeys
                // could possible be affected.
                deleteRow(session, oldRowData, true, false, tablesRequiringHKeyMaintenance, true);
                writeRow(session, mergedRowData, tablesRequiringHKeyMaintenance, true); // May throw DuplicateKeyException
            }
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        } finally {
            UPDATE_ROW_TAP.out();
            releaseExchange(session, hEx);
        }
    }

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

    private void updateIndex(Session session,
                             Index index,
                             RowDef rowDef,
                             RowData oldRowData,
                             RowData newRowData,
                             Key hKey,
                             PersistitIndexRowBuffer indexRowBuffer)
            throws PersistitException
    {
        checkNotGroupIndex(index);
        IndexDef indexDef = index.indexDef();
        if (!fieldsEqual(rowDef, oldRowData, newRowData, indexDef.getFields())) {
            TABLE_INDEX_MAINTENANCE_TAP.in();
            try {
                Exchange oldExchange = getExchange(session, index);
                deleteIndexRow(session, index, oldExchange, oldRowData, hKey, indexRowBuffer);
                Exchange newExchange = getExchange(session, index);
                constructIndexRow(newExchange, newRowData, index, hKey, indexRowBuffer, true);
                checkUniqueness(index, newRowData, newExchange);
                oldExchange.remove();
                newExchange.store();
                releaseExchange(session, newExchange);
                releaseExchange(session, oldExchange);
            } finally {
                TABLE_INDEX_MAINTENANCE_TAP.out();
            }
        }
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
    public void packRowData(Session session, Exchange ex, RowData rowData) {
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
    protected void clear(Session session, Exchange ex) {
        try {
            ex.remove();
        } catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
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
    public void buildIndexes(Session session, Collection<? extends Index> indexes, boolean defer) {
        Set<Group> groups = new HashSet<>();
        Map<Integer,RowDef> userRowDefs = new HashMap<>();
        Set<Index> indexesToBuild = new HashSet<>();
        for(Index index : indexes) {
            IndexDef indexDef = index.indexDef();
            if(indexDef == null) {
                throw new IllegalArgumentException("indexDef was null for index: " + index);
            }
            indexesToBuild.add(index);
            RowDef rowDef = indexDef.getRowDef();
            userRowDefs.put(rowDef.getRowDefId(), rowDef);
            groups.add(rowDef.table().getGroup());
        }
        PersistitIndexRowBuffer indexRow = new PersistitIndexRowBuffer(this);
        for (Group group : groups) {
            RowData rowData = new RowData(new byte[MAX_ROW_SIZE]);

            int indexKeyCount = 0;
            Exchange hEx = getExchange(session, group);
            try {
                hEx.clear();
                while (hEx.next(true)) {
                    expandRowData(hEx, rowData);
                    int tableId = rowData.getRowDefId();
                    RowDef userRowDef = userRowDefs.get(tableId);
                    if (userRowDef != null) {
                        for (Index index : userRowDef.getIndexes()) {
                            if(indexesToBuild.contains(index)) {
                                writeIndexRow(session, index, rowData, hEx.getKey(), indexRow);
                                indexKeyCount++;
                            }
                        }
                    }
                }
            } catch (PersistitException e) {
                throw new PersistitAdapterException(e);
            }
            LOG.debug("Inserted {} index keys into group {}", indexKeyCount, group.getName());
        }
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
    protected void preWriteRow(Session session, Exchange storeData, RowDef rowDef, RowData rowData) {
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
                fullTextService.dropIndex(session, (FullTextIndex)index);
                indexes.remove(index);
            }
        }
    }
    
    @Override
    public void deleteSequences (Session session, Collection<? extends Sequence> sequences) {
        removeTrees(session, sequences);
    }

    private RowData mergeRows(RowDef rowDef, RowData currentRow, RowData newRowData, ColumnSelector columnSelector) {
        NewRow mergedRow = NiceRow.fromRowData(currentRow, rowDef);
        NewRow newRow = new LegacyRowWrapper(rowDef, newRowData);
        int fields = rowDef.getFieldCount();
        for (int i = 0; i < fields; i++) {
            if (columnSelector.includesColumn(i)) {
                mergedRow.put(i, newRow.get(i));
            }
        }
        return mergedRow.toRowData();
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

    public <V extends IndexVisitor<Key,Value>> V traverse(Session session, Index index, V visitor) throws PersistitException {
        Exchange exchange = getExchange(session, index).append(Key.BEFORE);
        try {
            while (exchange.next(true)) {
                visitor.visit(exchange.getKey(), exchange.getValue());
            }
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
    public StoreAdapter createAdapter(Session session, Schema schema) {
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
