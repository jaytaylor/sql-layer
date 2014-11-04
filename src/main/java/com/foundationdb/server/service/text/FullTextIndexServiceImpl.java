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

package com.foundationdb.server.service.text;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.FullTextIndex;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Index.IndexType;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.ais.model.aisb2.AISBBasedBuilder;
import com.foundationdb.ais.model.aisb2.NewAISBuilder;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Cursor;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryBindings;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.SimpleQueryContext;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.row.AbstractRow;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.row.ValuesHKey;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.qp.util.SchemaCache;
import com.foundationdb.server.api.dml.scan.NiceRow;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.rowdata.RowData;
import com.foundationdb.server.service.Service;
import com.foundationdb.server.service.config.ConfigurationService;
import com.foundationdb.server.service.listener.ListenerService;
import com.foundationdb.server.service.listener.RowListener;
import com.foundationdb.server.service.listener.TableListener;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.session.SessionService;
import com.foundationdb.server.service.transaction.TransactionService;
import com.foundationdb.server.service.transaction.TransactionService.Callback;
import com.foundationdb.server.service.transaction.TransactionService.CallbackType;
import com.foundationdb.server.service.transaction.TransactionService.CloseableTransaction;
import com.foundationdb.server.store.SchemaManager;
import com.foundationdb.server.store.Store;
import com.foundationdb.sql.server.ServerCallContextStack;
import com.foundationdb.sql.server.ServerQueryContext;
import com.foundationdb.util.Exceptions;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;

import com.google.inject.Inject;
import com.persistit.Key;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class FullTextIndexServiceImpl extends FullTextIndexInfosImpl implements FullTextIndexService, Service, TableListener, RowListener
{
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceImpl.class);

    public static final String INDEX_PATH_PROPERTY = "fdbsql.text.indexpath";
    public static final String BACKGROUND_INTERVAL_PROPERTY = "fdbsql.text.backgroundInterval";

    private static final TableName CHANGES_TABLE = new TableName(TableName.INFORMATION_SCHEMA, "full_text_changes");
    private static final TableName BACKGROUND_WAIT_PROC_NAME = new TableName(TableName.SYS_SCHEMA, "full_text_background_wait");


    private final ConfigurationService configService;
    private final SessionService sessionService;
    private final ListenerService listenerService;
    private final SchemaManager schemaManager;
    private final Store store;
    private final TransactionService transactionService;
    private final Object BACKGROUND_CHANGE_LOCK = new Object();
    private final Object BACKGROUND_UPDATE_LOCK = new Object();

    private BackgroundRunner backgroundUpdate;
    private long backgroundInterval;
    private File indexPath;


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
    }

    //
    // FullTextIndexService
    //

    private void dropIndex(Session session, FullTextIndex index) {
        logger.trace("Delete {}", index.getIndexName());
        synchronized(BACKGROUND_UPDATE_LOCK) {
            FullTextIndexInfo info = getIndex(session, index.getIndexName(), index.getIndexedTable().getAIS());
            try {
                info.close();
            } catch(IOException e) {
                logger.error("Error closing index {} on drop", index.getIndexName(), e);
            }
            // Delete documents
            info.deletePath();
            synchronized(indexes) {
                indexes.remove(index.getIndexName());
            }
        }
    }

    @Override
    public RowCursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        FullTextIndexInfo index = getIndex(context.getSession(), name, null);
        try {
            return index.getSearcher().search(context, index.getHKeyRowType(),  query, limit);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error searching index", ex);
        }
    }

    @Override
    public void backgroundWait() {
        waitUpdateCycle();
    }


    //
    // Service
    //
    
    @Override
    public void start() {
        indexPath = new File(configService.getProperty(INDEX_PATH_PROPERTY));
        boolean success = indexPath.mkdirs();
        if(!success && !indexPath.exists()) {
            throw new AkibanInternalException("Could not create indexPath directories: " + indexPath);
        }

        registerSystemTables();
        listenerService.registerTableListener(this);
        listenerService.registerRowListener(this);

        backgroundInterval = Long.parseLong(configService.getProperty(BACKGROUND_INTERVAL_PROPERTY));
        enableUpdateWorker();
    }

    @Override
    public void stop() {
        disableUpdateWorker();

        listenerService.deregisterTableListener(this);
        listenerService.deregisterRowListener(this);

        synchronized(indexes) {
            for(FullTextIndexShared index : indexes.values()) {
                try {
                    index.close();
                } catch(IOException e) {
                    logger.warn("Error closing index {}", index.getName(), e);
                }
            }
            indexes.clear();
        }

        backgroundInterval = 0;
        indexPath = null;
    }

    @Override
    public void crash() {
        stop();
    }
    
    /* FullTextIndexInfosImpl */

    @Override
    protected File getIndexPath() {
        return indexPath;
    }

    @Override
    protected AkibanInformationSchema getAIS(Session session) {
        return schemaManager.getAis(session);
    }

    private void populateIndex(Session session, FullTextIndex index) {
        final FullTextIndexInfo indexInfo = getIndex(session, index.getIndexName(), index.getIndexedTable().getAIS());
        boolean success = false;
        try {
            StoreAdapter adapter = store.createAdapter(session, indexInfo.getSchema());
            QueryContext queryContext = new SimpleQueryContext(adapter);
            Cursor cursor = null;
            Indexer indexer = indexInfo.getIndexer();
            try(RowIndexer rowIndexer = new RowIndexer(indexInfo, indexer.getWriter(), false)) {
                cursor = API.cursor(indexInfo.fullScan(), queryContext, queryContext.createBindings());
                long count = rowIndexer.indexRows(cursor);
                logger.debug("Populated {} with {} rows", indexInfo.getIndex().getIndexName(), count);
            } finally {
                if(cursor != null && !cursor.isClosed()) {
                    cursor.close();
                }
            }
            transactionService.addCallback(session, CallbackType.COMMIT, new Callback() {
                @Override
                public void run(Session session, long timestamp) {
                    try {
                        indexInfo.commitIndexer();
                    } catch(IOException e) {
                        logger.error("Error committing index {}", indexInfo.getIndex().getIndexName(), e);
                    }
                }
            });
            success = true;
        } catch(IOException e) {
            throw new AkibanInternalException("Error populating index " + index, e);
        } finally {
            if(!success) {
                try {
                    indexInfo.rollbackIndexer();
                } catch(IOException e) {
                    logger.error("Error rolling back index population for {}", index, e);
                }
                synchronized(indexes) {
                    indexes.remove(index.getIndexName());
                }
            }
        }
    }

    private void updateIndex(Session session, FullTextIndexInfo indexInfo, Iterable<byte[]> rows) throws IOException {
        StoreAdapter adapter = store.createAdapter(session, indexInfo.getSchema());
        QueryContext queryContext = new SimpleQueryContext(adapter);
        QueryBindings queryBindings = queryContext.createBindings();

        Cursor cursor = null;
        IndexWriter writer = indexInfo.getIndexer().getWriter();
        try(RowIndexer rowIndexer = new RowIndexer(indexInfo, writer, true)) {
            Operator operator = indexInfo.getOperator();
            Iterator<byte[]> it = rows.iterator();
            while(it.hasNext()) {
                byte[] row = it.next();
                Row hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(), adapter);
                queryBindings.setRow(0, hkeyRow);
                cursor = API.cursor(operator, queryContext, queryBindings);
                rowIndexer.updateDocument(cursor, row);
                it.remove();
            }
        } finally {
            if(cursor != null && !cursor.isClosed()) {
                cursor.close();
            }
        }
    }


    //
    // TableListener
    //

    @Override
    public void onCreate(Session session, Table table) {
        for(Index index : table.getFullTextIndexes()) {
            populateIndex(session, (FullTextIndex)index);
        }
    }

    @Override
    public void onDrop(Session session, Table table) {
        for(FullTextIndex index : table.getFullTextIndexes()) {
            dropIndex(session, index);
        }
    }

    @Override
    public void onTruncate(Session session, Table table, boolean isFast) {
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
                populateIndex(session, (FullTextIndex)index);
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
    public void onInsertPost(Session session, Table table, Key hKey, Row row) {
        trackChange(session, table, hKey);
    }
    
    @Override
    public void onInsertPost(Session session, Table table, Key hKey, RowData row) {
        trackChange(session, table, hKey);
    }

    @Override
    public void onUpdatePre(Session session, Table table, Key hKey, RowData oldRow, RowData newRow) {
        // None
    }

    @Override
    public void onUpdatePost(Session session, Table table, Key hKey, RowData oldRow, RowData newRow) {
        trackChange(session, table, hKey);
    }

    @Override
    public void onDeletePre(Session session, Table table, Key hKey, RowData row) {
        trackChange(session, table, hKey);
    }


    // ---------- mostly for testing ---------------

    public void disableUpdateWorker() {
        synchronized(BACKGROUND_CHANGE_LOCK) {
            assert backgroundUpdate != null;
            backgroundUpdate.toFinished();
            backgroundUpdate = null;
        }
    }

    public void enableUpdateWorker() {
        synchronized(BACKGROUND_CHANGE_LOCK) {
            assert backgroundUpdate == null;
            backgroundUpdate = new BackgroundRunner("FullText_Update", backgroundInterval, new Runnable() {
                @Override
                public void run() {
                runUpdate();
                }
            });
            backgroundUpdate.start();
        }
    }

    public void waitUpdateCycle() {
        synchronized(BACKGROUND_CHANGE_LOCK) {
            if(backgroundUpdate != null) {
                backgroundUpdate.waitForCycle();
            }
        }
    }
    
    private void runUpdate() {
        // Consume and commit updates to each index in distinct blocks to keep r/w window small-ish
        for(;;) {
            try(Session session = sessionService.createSession();
                CloseableTransaction txn = transactionService.beginCloseableTransaction(session)) {
                // Quick exit if we won't see any
                if(changesRowCount(session) == 0) {
                    break;
                }
                // Only interact with FullTextIndexInfo under lock as to not fight concurrent DROP
                synchronized(BACKGROUND_UPDATE_LOCK) {
                    FullTextIndexInfo indexInfo = null;
                    try(HKeyBytesStream rows = new HKeyBytesStream(session)) {
                        if(rows.hasStream()) {
                            IndexName name = rows.getIndexName();
                            indexInfo = getIndexIfExists(session, name, null);
                            if(indexInfo == null) {
                                // Index has been deleted. Will conflict on so give up.
                                break;
                            } else {
                                updateIndex(session, indexInfo, rows);
                            }
                        }
                        txn.commit();
                        // Only commit changes to Lucene after successful iteration
                        // and removal of pending update rows
                        if(indexInfo != null) {
                            indexInfo.commitIndexer();
                            indexInfo = null;
                        }
                    } catch(IOException e) {
                        throw new AkibanInternalException("Error updating index", e);
                    } finally {
                        if(indexInfo != null) {
                            try {
                                indexInfo.rollbackIndexer();
                            } catch(IOException e) {
                                logger.warn( "Error rolling back update to {}", indexInfo.getIndex().getIndexName(), e);
                            }
                        }
                    }
                }
            }
        }
    }

    private Row toHKeyRow(byte rowBytes[], HKeyRowType hKeyRowType, StoreAdapter store)
    {
        HKey hkey = store.getKeyCreator().newHKey(hKeyRowType.hKey());
        hkey.copyFrom(rowBytes);
        if (hkey instanceof ValuesHKey) {
            return ((Row)(ValuesHKey)hkey);
        } else {
            throw new UnsupportedOperationException("HKey type is not ValuesHKey");
        }
    }

    private class HKeyBytesStream implements Iterable<byte[]>, Closeable
    {
        private IndexName indexName;
        private int indexID;
        private final Session session;
        private Cursor cursor;
        private Row row;

        private HKeyBytesStream(Session session) {
            this.session = session;
            AkibanInformationSchema ais = getAIS(session);
            Table changesTable = ais.getTable(CHANGES_TABLE);
            Operator plan = API.groupScan_Default(changesTable.getGroup());
            StoreAdapter adapter = store.createAdapter(session, SchemaCache.globalSchema(ais));
            QueryContext context = new SimpleQueryContext(adapter);
            this.cursor = API.cursor(plan, context, context.createBindings());
            cursor.open();
            findNextIndex();
        }

        private void findNextIndex() {
            indexName = null;
            while((row = cursor.next()) != null) {
                String schema = row.value(0).getString();
                String tableName = row.value(1).getString();
                String iName = row.value(2).getString();
                indexName = new IndexName(new TableName(schema, tableName), iName);
                indexID = row.value(3).getInt32();

                Table table = getAIS(session).getTable(indexName.getFullTableName());
                Index index = (table != null) ? table.getFullTextIndex(indexName.getName()) : null;
                // May have been deleted or recreated
                if(index != null && index.getIndexId() == indexID) {
                    break;
                }

                store.deleteRow(session, ((AbstractRow)row).rowData(), false);
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
        public void close() {
            if(cursor != null) {
                cursor.close();
                cursor = null;
                row = null;
                indexName = null;
            }
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
                   indexName.getSchemaName().equals(row.value(0).getString()) &&
                   indexName.getTableName().equals(row.value(1).getString()) &&
                   indexName.getName().equals(row.value(2).getString()) &&
                   indexID == row.value(3).getInt32()) {
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
                return row.value(4).getBytes();
            }

            @Override
            public void remove() {
                if(row == null) {
                    throw new IllegalStateException();
                }
                store.deleteRow(session, ((AbstractRow)row).rowData(), false);
            }
        }
    }

    private void trackChange(Session session, Table table, Key hKey) {
        NiceRow row = null;
        for(Index index : table.getFullTextIndexes()) {
            if(row == null) {
                AkibanInformationSchema ais = getAIS(session);
                Table changeTable = ais.getTable(CHANGES_TABLE);
                row = new NiceRow(changeTable.getTableId(), changeTable.rowDef());
            }
            row.put(0, index.getIndexName().getSchemaName());
            row.put(1, index.getIndexName().getTableName());
            row.put(2, index.getIndexName().getName());
            row.put(3, index.getIndexId());
            row.put(4, Arrays.copyOf(hKey.getEncodedBytes(), hKey.getEncodedSize()));
            store.writeNewRow(session, row);
        }
    }

    private long changesRowCount(Session session) {
        return store.getAIS(session).getTable(CHANGES_TABLE).rowDef().getTableStatus().getRowCount(session);
    }

    private void registerSystemTables() {
        final int identMax = 128;
        final int tableVersion = 1;
        final String schema = TableName.INFORMATION_SCHEMA;
        NewAISBuilder builder = AISBBasedBuilder.create(schema, schemaManager.getTypesTranslator());
        // TODO: Hidden PK too expensive?
        builder.table(CHANGES_TABLE)
               .colString("schema_name", identMax, false)
               .colString("table_name", identMax, false)
               .colString("index_name", identMax, false)
               .colInt("index_id", false)
               .colVarBinary("hkey", 4096, false);
        builder.procedure(BACKGROUND_WAIT_PROC_NAME)
               .language("java", Routine.CallingConvention.JAVA)
               .externalName(Routines.class.getName(), "backgroundWait");
        AkibanInformationSchema ais = builder.ais();
        schemaManager.registerStoredInformationSchemaTable(ais.getTable(CHANGES_TABLE), tableVersion);
        schemaManager.registerSystemRoutine(ais.getRoutine(BACKGROUND_WAIT_PROC_NAME));
    }

    @SuppressWarnings("unused") // Called reflectively
    public static class Routines {
        public static void backgroundWait() {
            ServerQueryContext context = ServerCallContextStack.getCallingContext();
            FullTextIndexService ft = context.getServer().getServiceManager().getServiceByClass(FullTextIndexService.class);
            ft.backgroundWait();
        }
    }


    public enum STATE {
        NOT_STARTED,
        RUNNING,
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
                try {
                    runInternal();
                } catch(Exception e) {
                    if(!Exceptions.isRollbackException(e)) {
                        logger.error("Run failed with exception", getName(), e);
                    }
                }
                sleep();
            }
        }

        public synchronized void toFinished() {
            checkRunning();
            state = STATE.STOPPING;
            sleepNotify();
            waitWhile(STATE.STOPPING, STATE.FINISHED);
            notifyAll();
        }

        public synchronized void waitForCycle() {
            checkRunning();
            // +2 ensures that we've observed one full run no matter where we initially started
            long target = runCount + 2;
            while(runCount < target) {
                sleepNotify();
                waitThenSet(null);
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

        private void runInternal() {
            runnable.run();
            synchronized(this) {
                ++runCount;
                notifyAll();
            }
        }

        private void checkRunning() {
            if(state == STATE.STOPPING || state == STATE.FINISHED) {
                throw new IllegalStateException("Not RUNNING: " + state);
            }
        }

        private void waitWhile(STATE whileState, STATE newState) {
            while(state == whileState) {
                waitThenSet(newState);
            }
        }

        private void waitThenSet(STATE newState) {
            try {
                wait();
                if(newState != null) {
                    state = newState;
                }
            } catch(InterruptedException e) {
                // None
            }
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
    }
}
