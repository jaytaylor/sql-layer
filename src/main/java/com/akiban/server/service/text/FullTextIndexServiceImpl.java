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

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.FullTextIndex;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Index.IndexType;
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.ais.model.aisb2.AISBBasedBuilder;
import com.akiban.ais.model.aisb2.NewAISBuilder;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryBindings;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.AbstractRow;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.qp.util.SchemaCache;
import com.akiban.server.api.dml.scan.NiceRow;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchRowException;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.rowdata.RowData;
import com.akiban.server.service.BackgroundWork;
import com.akiban.server.service.BackgroundWorkBase;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.listener.ListenerService;
import com.akiban.server.service.listener.RowListener;
import com.akiban.server.service.listener.TableListener;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.store.SchemaManager;
import com.akiban.server.store.Store;
import com.akiban.server.types3.mcompat.mfuncs.WaitFunctionHelpers;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;

import com.google.inject.Inject;
import com.persistit.exception.PersistitException;
import com.persistit.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


public class FullTextIndexServiceImpl extends FullTextIndexInfosImpl implements FullTextIndexService, Service, TableListener, RowListener
{
    public static final String INDEX_PATH_PROPERTY = "akserver.text.indexpath";
    public static final String UPDATE_INTERVAL = "akserver.text.maintenanceInterval";
    public static final String POPULATE_DELAY_INTERVAL = "akserver.text.populateDelayInterval";

    private static final TableName POPULATE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "full_text_populate");
    private static final TableName CHANGES_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "full_text_changes");


    private final ConfigurationService configService;
    private final SessionService sessionService;
    private final ListenerService listenerService;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TransactionService transactionService;

    private File indexPath;

    private Timer maintenanceTimer;
    private long maintenanceInterval;
    private volatile TimerTask updateWorker;

    private Timer populateTimer;
    private long populateDelayInterval;
    private ConcurrentHashMap<IndexName,Session> populating;
    
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceImpl.class);

    @Inject
    public FullTextIndexServiceImpl(ConfigurationService configService,
                                    SessionService sessionService,
                                    ListenerService listenerService,
                                    SchemaManager schemaManager,
                                    Store store,
                                    TransactionService transactionService) {
        this.configService = configService;
        this.sessionService = sessionService;
        this.listenerService = listenerService;
        this.schemaManager = schemaManager;
        this.store = store;
        this.transactionService = transactionService;
        this.populating = new ConcurrentHashMap<>();
    }

    //
    // FullTextIndexService
    //

    private void dropIndex(Session session, FullTextIndex idx) {
        logger.trace("Delete {}", idx.getIndexName());
        
        // This makes sure if we're dropping a newly created index 
        // the populate thread is stopped, and won't be restarting 
        // on the index we're dropping. 
        Session populatingSession = populating.putIfAbsent(idx.getIndexName(), session);
        if (populatingSession != null) {
            // if the population process is running, cancel it
            populatingSession.cancelCurrentQuery(true);
            // wait for the thread to complete 
            try {
                WaitFunctionHelpers.waitOn(Collections.singleton(POPULATE_BACKGROUND));
            } catch (InterruptedException e) {
                logger.error("waitOn populate during dropIndex failed", e);
            }
        } else {
            // delete 'promise' for population, if any
            try {
                store.deleteRow(session, createPopulateRow(session, idx.getIndexName()), true, false);
            } catch(NoSuchRowException e) {
                // Acceptable
            }
        }
        
        // This deals with the update thread. If the update
        // is running, wait for it to complete. Since the update
        // process mixes different indexes in the same update, there's
        // no good way to tell if our index is being updated. 
        if (updateRunning) {
            try {
                WaitFunctionHelpers.waitOn(Collections.singleton(UPDATE_BACKGROUND));
            } catch (InterruptedException e) {
                logger.error("waitOn update during dropIndex failed", e);
            }
        }
        
        // delete documents
        FullTextIndexInfo idxInfo = getIndex(session, idx.getIndexName(), idx.getIndexedTable().getAIS());
        idxInfo.deletePath();
        synchronized (indexes) {    
            indexes.remove(idx.getIndexName());
        }
        populating.remove(idx.getIndexName());        
    }

    @Override
    public Cursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        FullTextIndexInfo index = getIndex(context.getSession(), name, null);
        try {
            return index.getSearcher().search(context, index.getHKeyRowType(), 
                                              query, limit);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error searching index", ex);
        }
    }

    @Override
    public List<? extends BackgroundWork> getBackgroundWorks()
    {
        return Arrays.asList(POPULATE_BACKGROUND, UPDATE_BACKGROUND);
    }
    
    /* Service */
    
    @Override
    public void start() {
        registerSystemTables();
        enableUpdateWorker();
        enablePopulateWorker();
        listenerService.registerTableListener(this);
        listenerService.registerRowListener(this);
    }

    @Override
    public void stop() {
        listenerService.deregisterTableListener(this);
        listenerService.deregisterRowListener(this);
        try {
            for (FullTextIndexShared index : indexes.values()) {
                index.close();
            }
            disableUpdateWorker();
            disablePopulateWorker();
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error closing index", ex);
        }
    }

    @Override
    public void crash() {
        stop();
    }
    
    /* FullTextIndexInfosImpl */

    @Override
    protected synchronized File getIndexPath() {
        if (indexPath == null) {
            indexPath = new File(configService.getProperty(INDEX_PATH_PROPERTY));
            boolean success = indexPath.mkdirs();
            if (!success && !indexPath.exists()) {
                throw new AkibanInternalException("Could not create indexPath directories: " + indexPath);
            }
        }
        return indexPath;
    }

    @Override
    protected AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    protected long populateIndex(Session session, FullTextIndexInfo index) throws IOException {
        Indexer indexer = index.getIndexer();
        Operator plan = index.fullScan();
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = store.createAdapter(session, index.getSchema());
        QueryContext queryContext = new SimpleQueryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();
        IndexWriter writer = indexer.getWriter();

        Cursor cursor = null;
        boolean success = false;
        try(RowIndexer rowIndexer = new RowIndexer(index, writer, false)) {
            cursor = API.cursor(plan, queryContext, queryBindings);
            long count = rowIndexer.indexRows(cursor);
            writer.commit();
            success = true;
            return count;
        }
        finally {
            if(cursor != null) {
                cursor.close();
                cursor.destroy();
            }
            if(!success) {
                writer.rollback();
            }
        }
    }

    private void updateIndex(Session session, IndexName name, Iterable<byte[]> rows) {
        try
        {
            FullTextIndexInfo indexInfo = getIndex(session, name, null);
            StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
            if (adapter == null)
                adapter = store.createAdapter(session, indexInfo.getSchema());
            QueryContext queryContext = new SimpleQueryContext(adapter);
            QueryBindings queryBindings = queryContext.createBindings();
            HKeyCache<com.akiban.qp.row.HKey> cache = new HKeyCache<>(adapter);
            IndexWriter writer = indexInfo.getIndexer().getWriter();

            Cursor cursor = null;
            boolean success = false;
            try(RowIndexer rowIndexer = new RowIndexer(indexInfo, writer, true))
            {
                Operator operator = indexInfo.getOperator();
                Iterator<byte[]> it = rows.iterator();
                while(it.hasNext()) {
                    byte[] row = it.next();
                    HKeyRow hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(), adapter, cache);
                    queryBindings.setRow(0, hkeyRow);
                    cursor = API.cursor(operator, queryContext, queryBindings);
                    rowIndexer.updateDocument(cursor, row);
                    it.remove();
                }
                writer.commit();
                success = true;

            }
            finally
            {
                if(cursor != null) {
                    cursor.close();
                    cursor.destroy();
                }
                if(!success) {
                    writer.rollback();
                }
            }
        }
        catch (IOException e)
        {
            throw new AkibanInternalException("Error updating index", e);
        }
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, UserTable table) {
        for(Index index : table.getFullTextIndexes()) {
            trackPopulate(session, index.getIndexName());
        }
    }

    @Override
    public void onDrop(Session session, UserTable table) {
        for(FullTextIndex index : table.getFullTextIndexes()) {
            dropIndex(session, index);
        }
    }

    @Override
    public void onTruncate(Session session, UserTable table, boolean isFast) {
        if(isFast) {
            for(FullTextIndex index : table.getFullTextIndexes()) {
                if(index.getIndexType() == IndexType.FULL_TEXT) {
                    throw new IllegalStateException("Cannot fast truncate: " + index);
                }
            }
        }
    }

    @Override
    public void onCreateIndex(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            if(index.getIndexType() == IndexType.FULL_TEXT) {
                trackPopulate(session, index.getIndexName());
            }
        }
    }

    @Override
    public void onDropIndex(Session session, Collection<? extends Index> indexes) {
        for(Index index : indexes) {
            if(index.getIndexType() == IndexType.FULL_TEXT) {
                dropIndex(session, (FullTextIndex)index);
            }
        }
    }


    //
    // RowListener
    //

    @Override
    public void onWrite(Session session, UserTable table, Key hKey, RowData row) {
        trackChange(session, table, hKey);
    }

    @Override
    public void onUpdate(Session session, UserTable table, Key hKey, RowData oldRow, RowData newRow) {
        trackChange(session, table, hKey);
    }

    @Override
    public void onDelete(Session session, UserTable table, Key hKey, RowData row) {
        trackChange(session, table, hKey);
    }


    private volatile boolean updateRunning = false;
    private class DefaultUpdateWorker extends TimerTask
    {
        @Override
        public void run()
        {
            runUpdate();
        }
    }

    final BackgroundWork POPULATE_BACKGROUND = new BackgroundWorkBase()
    {
        @Override
        public boolean forceExecution()
        {
            return forcePopulate();
        }

        @Override
        public long getMinimumWaitTime()
        {
            return populateEnabled && hasScheduled
                    ? populateDelayInterval
                    : 0;
        }

        @Override
        public String toString()
        {
            return "POPULATE";
        }
    };

    final BackgroundWork UPDATE_BACKGROUND = new BackgroundWorkBase()
    {
        @Override
        public boolean forceExecution()
        {
            return forceUpdate();
        }

        @Override
        public long getMinimumWaitTime()
        {
            return updateWorker == null
                    ? 0 // worker is disabled.
                    : maintenanceInterval;
        }

        @Override
        public String toString()
        {
            return "UPDATE";
        }
    };

    private volatile boolean populateRunning = false;
    private class DefaultPopulateWorker extends TimerTask
    {
        @Override
        public void run()
        {
            runPopulate();
        }
    }

    // ---------- mostly for testing ---------------
    void disableUpdateWorker()
    {
        if (maintenanceTimer == null)
        {
            logger.debug("maintenance worker ALREADY disabled");
            return;
        }
        updateWorker.cancel();
        maintenanceTimer.cancel();
        maintenanceTimer.purge();
        maintenanceTimer = null;
        updateWorker = null;
    }
    
    protected void enableUpdateWorker()
    {
        maintenanceInterval = Long.parseLong(configService.getProperty(UPDATE_INTERVAL));
        maintenanceTimer = new Timer();
        updateWorker = new DefaultUpdateWorker();
        maintenanceTimer.scheduleAtFixedRate(updateWorker, maintenanceInterval, maintenanceInterval);
    }
    
    protected void enablePopulateWorker()
    {
        populateDelayInterval = Long.parseLong(configService.getProperty(POPULATE_DELAY_INTERVAL));
        populateTimer = new Timer();
        populateTimer.schedule(populateWorker(), populateDelayInterval);
        populateEnabled = true;
        hasScheduled = true;
    }
    
    void disablePopulateWorker()
    {
        if (populateTimer == null)
        {
            logger.debug("populate worker already disabled");
            return;
        }
        populateTimer.cancel();
        populateTimer.purge();
        hasScheduled = false;
        populateEnabled = false;
        populateTimer = null;
    }

    Cursor populateTableCursor(Session session) {
        AkibanInformationSchema ais = getAIS(session);
        UserTable populateTable = ais.getUserTable(POPULATE_TABLE);
        StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(ais));
        QueryContext context = new SimpleQueryContext(adapter);
        Operator plan = API.groupScan_Default(populateTable.getGroup());
        return API.cursor(plan, context, context.createBindings());
    }

    protected boolean populateNextIndex(Session session) throws PersistitException
    {
        transactionService.beginTransaction(session);
        Cursor cursor = null;
        IndexName toPopulate = null;
        try {
            cursor = populateTableCursor(session);
            cursor.open();
            toPopulate = nextInQueue(session, cursor, false);
            if(toPopulate != null && stillExists (session, toPopulate)) {
                FullTextIndexInfo index = getIndex(session, toPopulate, null);
                populateIndex(session, index);
                store.deleteRow(session, createPopulateRow(session, toPopulate), true, false);
                transactionService.commitTransaction(session);
                populating.remove(toPopulate);
                return true;
            }
        } catch (QueryCanceledException e2) {
            // The query could be canceled if the user drops the index  
            // while this thread is populating the index
            // The Lock Obtained failed exception occurs for the same reason.
            //  we're trying to delete the index at the same time as the 
            // populate is running, but with slightly different timing. 
            // Clean up after ourselves. 
            if(cursor != null) {
                cursor.close();
            }
            populating.remove(toPopulate);
            transactionService.commitTransaction(session);
            // start another thread to make sure we're not missing anything
            populateTimer.schedule(populateWorker(), populateDelayInterval);
            hasScheduled = true;
            logger.warn("populateNextIndex aborted : {}", e2.getMessage());
        } catch (IOException ioex) {
            throw new AkibanInternalException ("Failed to populate index ", ioex);
        } finally {
            transactionService.rollbackTransactionIfOpen(session);
        }
        return false;
    }
    
    protected synchronized void runPopulate()
    {
        populateRunning = true;
        Session session = sessionService.createSession();
        try
        {
            boolean more = true;
            while(more) {
                more = populateNextIndex(session);
            }
        }
        catch (PersistitException ex1)
        {
            throw PersistitAdapter.wrapPersistitException(session, ex1);
        }
        finally
        {
            hasScheduled = false;
            POPULATE_BACKGROUND.notifyObservers();
            populateRunning = false;
            session.close();
        }
    }
    
    private boolean stillExists(Session session, IndexName indexName)
    {
        AkibanInformationSchema ais = getAIS(session);
        UserTable table = ais.getUserTable(indexName.getFullTableName());
        return !(table == null ||  table.getFullTextIndex(indexName.getName()) == null);
    }
    
    private synchronized void runUpdate()
    {
        updateRunning = true;
        Session session = sessionService.createSession();
        HKeyBytesStream rows = null;
        try
        {
            // Consume and commit updates to each index in distinct blocks to keep r/w window small-ish
            boolean done = false;
            while(!done) {
                transactionService.beginTransaction(session);
                rows = getChangedRows(session);
                if(rows.hasStream()) {
                    if(populating.get(rows.getIndexName()) == null) {
                        updateIndex(session, rows.getIndexName(), rows);
                    }
                } else {
                    done = true;
                }
                transactionService.commitTransaction(session);
            }

        }
        catch(Exception e)
        {
            logger.error("Error while maintaining full_text indices", e);
        }
        finally
        {
            if(rows != null && rows.cursor != null) {
                rows.cursor.close();
            }
            transactionService.rollbackTransactionIfOpen(session);
            session.close();
            UPDATE_BACKGROUND.notifyObservers();
            updateRunning = false;
        }
    }

    private boolean forceUpdate()
    {
        // worker is disabled or is already running
        if (updateWorker == null || updateRunning)
            return false;

        // execute the thread in different 
        // thread (so it has a new session)
        new Thread(updateWorker).start();
        return true;
    }
    
    
    /**
     * If the populate job is not already running, force the worker to wake
     * up and do its job immediately.
     * 
     * If the worker is disabled, return false and do nothing.
     */
    private boolean forcePopulate()
    {
        // no work to do
        // or it is already being done
        if (!populateEnabled || !hasScheduled || populateRunning)
            return false;

        // block the timer (so other threads would have to wait)
        // Unlike the update case, this is needed because population does not
        // have to be done  periodically
        // So unless there are new index created, we don't need to
        // execute the task again after this execution

        // cancel scheduled task
        populateTimer.cancel();

        // execute the task
        // in a different thread
        // because we'd otherwise get "transaction already began" exception
        //  as each thread only has one session)
        new Thread(populateWorker()).start();
        hasScheduled = true;
        // get a new timer
        // (So schedulePopulate can schedule new task if new index is created)
        populateTimer = new Timer();
       
        return true;
    }

    private synchronized TimerTask populateWorker ()
    {
        return new DefaultPopulateWorker();
    }

    IndexName nextInQueue(Session session, Cursor cursor, boolean traversing) {
        Row row;
        while((row = cursor.next()) != null) {
            String schema = row.pvalue(0).getString();
            String table = row.pvalue(1).getString();
            String index = row.pvalue(2).getString();
            IndexName ret = new IndexName(new TableName(schema, table), index);
            // The populating map contains the indexes currently being built
            // if this name is already in the tree, skip this one, and try
            // the next.
            if (!traversing) {
                if (populating.putIfAbsent(ret, session) != null) {
                    continue;
                }
            }
            return ret;
        }
        return null;
    }

    private volatile boolean hasScheduled = false;
    private volatile boolean populateEnabled = false;

    private HKeyRow toHKeyRow(byte rowBytes[], HKeyRowType hKeyRowType,
                              StoreAdapter store, HKeyCache<com.akiban.qp.row.HKey> cache)
    {
        PersistitHKey hkey = store.newHKey(hKeyRowType.hKey());
        Key key = hkey.key();
        key.setEncodedSize(rowBytes.length);
        System.arraycopy(rowBytes, 0, key.getEncodedBytes(), 0, rowBytes.length);
        return new HKeyRow(hKeyRowType, hkey, cache);
    }

    public HKeyBytesStream getChangedRows(Session session) {
        return new HKeyBytesStream(session);
    }

    private class HKeyBytesStream implements Iterable<byte[]>
    {
        private IndexName indexName;
        private int indexID;
        private final Session session;
        private Cursor cursor;
        private Row row;

        private HKeyBytesStream(Session session) {
            this.session = session;
            findNextIndex();
        }

        private void findNextIndex() {
            indexName = null;
            cursor = null;

            AkibanInformationSchema ais = getAIS(session);
            UserTable changesTable = ais.getUserTable(CHANGES_TABLE);
            Operator plan = API.groupScan_Default(changesTable.getGroup());
            StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(ais));
            QueryContext context = new SimpleQueryContext(adapter);
            cursor = API.cursor(plan, context, context.createBindings());
            cursor.open();
            while((row = cursor.next()) != null) {
                String schema = row.pvalue(0).getString();
                String tableName = row.pvalue(1).getString();
                String iName = row.pvalue(2).getString();
                indexName = new IndexName(new TableName(schema, tableName), iName);
                indexID = row.pvalue(3).getInt32();

                UserTable table = getAIS(session).getUserTable(indexName.getFullTableName());
                Index index = (table != null) ? table.getFullTextIndex(indexName.getName()) : null;
                // May have been deleted or recreated
                if(index != null && index.getIndexId() == indexID) {
                    break;
                }

                store.deleteRow(session, ((AbstractRow)row).rowData(), true, false);
                indexName = null;
                row = null;
            }
        }

        public boolean hasStream() {
            return indexName != null;
        }

        public IndexName getIndexName() {
            return indexName;
        }

        @Override
        public Iterator<byte[]> iterator() {
            return new StreamIterator(hasStream(), row);
        }

        private class StreamIterator implements Iterator<byte[]> {
            private Boolean hasNext;
            private Row row;

            private StreamIterator(boolean hasNext, Row row) {
                this.hasNext = hasNext;
                this.row = row;
            }

            private void advance() {
                row = cursor.next();
                if(row != null &&
                   indexName.getSchemaName().equals(row.pvalue(0).getString()) &&
                   indexName.getTableName().equals(row.pvalue(1).getString()) &&
                   indexName.getName().equals(row.pvalue(2).getString()) &&
                   indexID == row.pvalue(3).getInt32()) {
                    hasNext = true;
                } else {
                    hasNext = false;
                    row = null;
                }
            }

            @Override
            public boolean hasNext() {
                if(hasNext == null) {
                    advance();
                }
                return hasNext;
            }

            @Override
            public byte[] next() {
                if(hasNext == null) {
                    advance();
                }
                if(!hasNext) {
                    throw new NoSuchElementException();
                }
                hasNext = null;
                byte[] bytes = row.pvalue(4).getBytes();
                return bytes;
            }

            @Override
            public void remove() {
                if(row == null) {
                    throw new IllegalStateException();
                }
                store.deleteRow(session, ((AbstractRow)row).rowData(), true, false);
            }
        }
    }

    private RowData createPopulateRow(Session session, IndexName indexName) {
        AkibanInformationSchema ais = getAIS(session);
        UserTable changeTable = ais.getUserTable(POPULATE_TABLE);
        NiceRow row = new NiceRow(changeTable.getTableId(), changeTable.rowDef());
        row.put(0, indexName.getSchemaName());
        row.put(1, indexName.getTableName());
        row.put(2, indexName.getName());
        return row.toRowData();
    }

    private void trackPopulate(Session session, IndexName indexName) {
        store.writeRow(session, createPopulateRow(session, indexName), null);

        // TODO: What if this fires before this row is committed?
        // if there are no scheduled populate workers running, add one to run shortly.
        if(populateEnabled && !hasScheduled) {
            populateTimer.schedule(populateWorker(), populateDelayInterval);
            hasScheduled = true;
        }
    }

    private void trackChange(Session session, UserTable table, Key hKey) {
        NiceRow row = null;
        for(Index index : table.getFullTextIndexes()) {
            if(row == null) {
                AkibanInformationSchema ais = getAIS(session);
                UserTable changeTable = ais.getUserTable(CHANGES_TABLE);
                row = new NiceRow(changeTable.getTableId(), changeTable.rowDef());
            }
            row.put(0, index.getIndexName().getSchemaName());
            row.put(1, index.getIndexName().getTableName());
            row.put(2, index.getIndexName().getName());
            row.put(3, index.getIndexId());
            row.put(4, Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize()));
            store.writeRow(session, row.toRowData(), null);
        }
    }

    private void registerSystemTables() {
        final int identMax = 128;
        final int tableVersion = 1;
        final String schema = TableName.INFORMATION_SCHEMA;
        NewAISBuilder builder = AISBBasedBuilder.create(schema);
        builder.userTable(POPULATE_TABLE)
               .colString("schema_name", identMax, false)
               .colString("table_name", identMax, false)
               .colString("index_name", identMax, false)
               .pk("schema_name", "table_name", "index_name");
        // TODO: Hidden PK too expensive?
        builder.userTable(CHANGES_TABLE)
               .colString("schema_name", identMax, false)
               .colString("table_name", identMax, false)
               .colString("index_name", identMax, false)
               .colLong("index_id", false)
               .colVarBinary("hkey", 4096, false);
        AkibanInformationSchema ais = builder.ais();
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(POPULATE_TABLE), tableVersion);
        schemaManager.registerStoredInformationSchemaTable(ais.getUserTable(CHANGES_TABLE), tableVersion);
    }
}
