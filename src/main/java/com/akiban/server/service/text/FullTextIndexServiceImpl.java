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
import com.akiban.qp.operator.RowCursor;
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
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;


public class FullTextIndexServiceImpl extends FullTextIndexInfosImpl implements FullTextIndexService, Service, TableListener, RowListener
{
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceImpl.class);

    public static final String INDEX_PATH_PROPERTY = "akserver.text.indexpath";
    public static final String BACKGROUND_INTERVAL = "akserver.text.backgroundInterval";

    private static final TableName POPULATE_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "full_text_populate");
    private static final TableName CHANGES_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "full_text_changes");


    private final ConfigurationService configService;
    private final SessionService sessionService;
    private final ListenerService listenerService;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TransactionService transactionService;

    private File indexPath;

    private volatile IndexName updatingIndex;
    private BackgroundRunner backgroundPopulate;
    private BackgroundRunner backgroundUpdate;
    private ConcurrentHashMap<IndexName,Session> populating;


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
            backgroundPopulate.waitForCycle();
        } else {
            // delete 'promise' for population, if any
            try {
                store.deleteRow(session, createPopulateRow(session, idx.getIndexName()), true, false);
            } catch(NoSuchRowException e) {
                // Acceptable
            }
        }
        
        // If updatingIndex is currently equal, wait for it to change.
        // If it isn't, or once it does, it can never become equal as it now exists in the populating map.
        if(idx.getIndexName().equals(updatingIndex)) {
            backgroundUpdate.waitForCycle();
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
    public RowCursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        FullTextIndexInfo index = getIndex(context.getSession(), name, null);
        try {
            return index.getSearcher().search(context, index.getHKeyRowType(), 
                                              query, limit);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error searching index", ex);
        }
    }

    
    //
    // Service
    //
    
    @Override
    public void start() {
        registerSystemTables();
        listenerService.registerTableListener(this);
        listenerService.registerRowListener(this);

        long backgroundInterval =  Long.parseLong(configService.getProperty(BACKGROUND_INTERVAL));
        backgroundUpdate = new BackgroundRunner("FullText_Update", backgroundInterval, new Runnable() {
            @Override
            public void run() {
                runUpdate();
            }
        });
        backgroundUpdate.start();
        backgroundPopulate = new BackgroundRunner("FullText_Populate", backgroundInterval, new Runnable() {
            @Override
            public void run() {
                runPopulate();
            }
        });
        backgroundPopulate.start();
    }

    @Override
    public void stop() {
        backgroundUpdate.toFinished();
        backgroundPopulate.toFinished();

        listenerService.deregisterTableListener(this);
        listenerService.deregisterRowListener(this);

        try {
            for (FullTextIndexShared index : indexes.values()) {
                index.close();
            }
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
            IndexWriter writer = indexInfo.getIndexer().getWriter();

            Cursor cursor = null;
            boolean success = false;
            try(RowIndexer rowIndexer = new RowIndexer(indexInfo, writer, true))
            {
                Operator operator = indexInfo.getOperator();
                Iterator<byte[]> it = rows.iterator();
                while(it.hasNext()) {
                    byte[] row = it.next();
                    HKeyRow hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(), adapter);
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


    // ---------- mostly for testing ---------------

    public void disableUpdateWorker() {
        backgroundUpdate.toPaused();
    }

    public void enableUpdateWorker() {
        backgroundUpdate.toRunning();
    }

    public void disablePopulateWorker() {
        backgroundPopulate.toPaused();
    }

    public void enablePopulateWorker() {
        backgroundPopulate.toRunning();
    }

    public void waitPopulateCycle() {
        if(backgroundPopulate.state != STATE.PAUSED) {
            backgroundPopulate.waitForCycle();
        }
    }

    public void waitUpdateCycle() {
        if(backgroundUpdate.state != STATE.PAUSED) {
            backgroundUpdate.waitForCycle();
        }
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
            // Quick exit if we wont' see any this transaction
            if(populateRowCount(session) == 0) {
                return false;
            }
            cursor = populateTableCursor(session);
            cursor.open();
            toPopulate = nextInQueue(session, cursor, false);
            if(toPopulate != null && stillExists (session, toPopulate)) {
                FullTextIndexInfo index = getIndex(session, toPopulate, null);
                populateIndex(session, index);
                store.deleteRow(session, createPopulateRow(session, toPopulate), true, false);
                populating.remove(toPopulate);
                transactionService.commitTransaction(session);
                return true;
            }
        } catch (QueryCanceledException e2) {
            // The query could be canceled if the user drops the index  
            // while this thread is populating the index.
            // Clean up after ourselves.
            if(cursor != null) {
                cursor.close();
            }
            populating.remove(toPopulate);
            logger.warn("populateNextIndex aborted : {}", e2.getMessage());
        } catch (IOException e) {
            throw new AkibanInternalException ("Failed to populate index ", e);
        } finally {
            transactionService.rollbackTransactionIfOpen(session);
        }
        return false;
    }
    
    protected synchronized void runPopulate() {
        Session session = sessionService.createSession();
        try {
            boolean more = true;
            while(more) {
                more = populateNextIndex(session);
            }
        }
        catch(PersistitException e) {
            throw PersistitAdapter.wrapPersistitException(session, e);
        }
        finally {
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
        Session session = sessionService.createSession();
        HKeyBytesStream rows = null;
        try {
            // Consume and commit updates to each index in distinct blocks to keep r/w window small-ish
            boolean done = false;
            while(!done) {
                transactionService.beginTransaction(session);
                // Quick exit if we won't see any
                if(changesRowCount(session) == 0) {
                    done = true;
                } else {
                    rows = getChangedRows(session);
                    if(rows.hasStream()) {
                        IndexName name = rows.getIndexName();
                        if(populating.get(name) == null) {
                            updatingIndex = name;
                            updateIndex(session, name, rows);
                        }
                        rows.cursor.close();
                        rows = null;
                        updatingIndex = null;
                    } else {
                        done = true;
                    }
                }
                transactionService.commitTransaction(session);
            }
        }
        catch(Exception e) {
            logger.error("Error while maintaining full_text indices", e);
        }
        finally {
            if(rows != null && rows.cursor != null) {
                rows.cursor.close();
            }
            updatingIndex = null;
            transactionService.rollbackTransactionIfOpen(session);
            session.close();
        }
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

    private HKeyRow toHKeyRow(byte rowBytes[], HKeyRowType hKeyRowType,
                              StoreAdapter store)
    {
        PersistitHKey hkey = store.newHKey(hKeyRowType.hKey());
        Key key = hkey.key();
        key.setEncodedSize(rowBytes.length);
        System.arraycopy(rowBytes, 0, key.getEncodedBytes(), 0, rowBytes.length);
        return new HKeyRow(hKeyRowType, hkey, new HKeyCache<>(store));
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

    private long populateRowCount(Session session) {
        return store.getAIS(session).getUserTable(POPULATE_TABLE).rowDef().getTableStatus().getRowCount(session);
    }

    private long changesRowCount(Session session) {
        return store.getAIS(session).getUserTable(CHANGES_TABLE).rowDef().getTableStatus().getRowCount(session);
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


    public enum STATE {
        NOT_STARTED,
        RUNNING, PAUSED,
        STOPPING,
        FINISHED
    }

    public class BackgroundRunner extends Thread {
        private final Object SLEEP_MONITOR = new Object();
        private final Runnable runnable;
        private final long sleepMillis;
        private volatile STATE state;
        private long runCount;

        public BackgroundRunner(String name, long sleepMillis, Runnable runnable) {
            super(name);
            this.runnable = runnable;
            this.sleepMillis = sleepMillis;
            this.state = STATE.NOT_STARTED;
        }

        @Override
        public void run() {
            state = STATE.RUNNING;
            while(state != STATE.STOPPING) {
                waitFor(STATE.PAUSED, true, null);
                runnable.run();
                synchronized(this) {
                    ++runCount;
                    notifyAll();
                }
                sleep();
            }
        }

        public void toPaused() {
            fromTo(STATE.RUNNING, false, STATE.PAUSED);
        }

        public void toRunning() {
            fromTo(STATE.PAUSED, false, STATE.RUNNING);
        }

        public void toFinished() {
            fromTo(null, false, STATE.STOPPING);
            fromTo(STATE.STOPPING, false, STATE.FINISHED);
        }

        public synchronized void waitForCycle() {
            if(state != STATE.RUNNING) {
                throw new IllegalStateException("Not RUNNING");
            }
            // +2 ensures that we've observed one full run no matter where we initially started
            long target = runCount + 2;
            while(runCount < target) {
                sleepNotify();
                waitInternal(null);
            }
        }

        public void sleepNotify() {
            synchronized(SLEEP_MONITOR) {
                SLEEP_MONITOR.notify();
            }
        }

        //
        // Helpers
        //

        private synchronized void waitFor(STATE whileState, boolean equal, STATE newState) {
            while((state == whileState) == equal) {
                waitInternal(newState);
            }
        }

        private synchronized void fromTo(STATE fromState, boolean equal, STATE newState) {
            if(fromState != null && state != fromState) {
                throw new IllegalStateException("Expected " + fromState);
            }
            waitFor(fromState, equal, newState);
            state = newState;
            notifyAll();
        }

        private void sleep() {
            try {
                synchronized(SLEEP_MONITOR) {
                    SLEEP_MONITOR.wait(sleepMillis);
                }
            } catch(InterruptedException e) {
                // None
            }
        }

        private void waitInternal(STATE newState) {
            try {
                wait();
                if(newState != null) {
                    state = newState;
                }
            } catch(InterruptedException e) {
                // None
            }
        }
    }
}
