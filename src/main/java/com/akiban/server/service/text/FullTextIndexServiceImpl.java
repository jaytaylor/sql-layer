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
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.QueryCanceledException;
import com.akiban.server.service.BackgroundWork;
import com.akiban.server.service.BackgroundWorkBase;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.session.SessionService;
import com.akiban.server.service.session.SessionServiceImpl;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.PersistitStore;
import com.akiban.server.store.PersistitStore.HKeyBytesStream;
import com.akiban.server.store.Store;
import com.akiban.server.types3.mcompat.mfuncs.WaitFunctionHelpers;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;

import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;


public class FullTextIndexServiceImpl extends FullTextIndexInfosImpl implements FullTextIndexService, Service {
    public static final String INDEX_PATH_PROPERTY = "akserver.text.indexpath";
    public static final String UPDATE_INTERVAL = "akserver.text.maintenanceInterval";
    public static final String POPULATE_DELAY_INTERVAL = "akserver.text.populateDelayInterval";

    private static final String POPULATE_SCHEMA = "populate";
    private static final String FULL_TEXT_TABLE = "full_text_populate";
    
    private final SessionService sessionService = new SessionServiceImpl();


    private final ConfigurationService configService;
    private final DXLService dxlService;
    private final Store store;
    private final TransactionService transactionService;
    private final TreeService treeService;

    private File indexPath;

    private Timer maintenanceTimer;
    private long maintenanceInterval;
    private TimerTask updateWorker;
    private PersistitStore persistitStore;

    private Timer populateTimer;
    private long populateDelayInterval;
    private ConcurrentHashMap<IndexName,Session> populating;
    
    private static final Logger logger = LoggerFactory.getLogger(FullTextIndexServiceImpl.class);

    @Inject
    public FullTextIndexServiceImpl(ConfigurationService configService,
                                    DXLService dxlService, Store store,
                                    TransactionService transactionService,
                                    TreeService treeService) {
        this.configService = configService;
        this.dxlService = dxlService;
        this.store = store;
        this.transactionService = transactionService;
        this.treeService = treeService;
        this.populating = new ConcurrentHashMap<> ();
    }

    /* FullTextIndexService */

    private long createIndex(Session session, IndexName name) throws IOException {
        FullTextIndexInfo index = getIndex(session, name, null);
        return populateIndex(session, index);
    }

    void deleteFromTree(Session session, IndexName name)
    {
        try
        {
            Exchange ex = getPopulateExchange(session);
            ex.clear().append(name.getSchemaName())
                      .append(name.getTableName())
                      .append(name.getName());
            
            if (ex.traverse(Key.Direction.EQ, true, 0))
                ex.fetchAndRemove();
        }
        catch (PersistitException e)
        {
            throw new AkibanInternalException("Error while removing index", e);
        }
    }
    
    @Override
    public void dropIndex(Session session, FullTextIndex idx) {

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
                WaitFunctionHelpers.waitOn(Collections.singletonList(getBackgroundWorks().get(0)));
            } catch (InterruptedException e) {
                ;// TODO: do nothing
            }
        } else {
            // delete 'promise' for population, if any
            deleteFromTree(session, idx.getIndexName());

        }
        
        // This deals with the update thread. If the update
        // is running, wait for it to complete. Since the update
        // process mixes different indexes in the same update, there's
        // no good way to tell if our index is being updated. 
        if (updateRunning) {
            try {
                WaitFunctionHelpers.waitOn(Collections.singletonList(getBackgroundWorks().get(1)));
            } catch (InterruptedException e) {
                ;//TODO: do nothing
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
        return backgroundWorks;
    }
    
    /* Service */
    
    @Override
    public void start() {
        persistitStore = store.getPersistitStore();
        persistitStore.setFullTextService(this);
        enableUpdateWorker();
        enablePopulateWorker();
    }

    @Override
    public void stop() {
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
            indexPath.mkdirs();
        }
        return indexPath;
    }

    @Override
    protected AkibanInformationSchema getAIS(Session session) {
        return dxlService.ddlFunctions().getAIS(session);
    }

    protected long populateIndex(Session session, FullTextIndexInfo index)
            throws IOException {
        Indexer indexer = index.getIndexer();
        Operator plan = index.fullScan();
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = store.createAdapter(session, index.getSchema());
        QueryContext queryContext = new SimpleQueryContext(adapter);
        IndexWriter writer = indexer.getWriter();
        RowIndexer rowIndexer = new RowIndexer(index, writer, false);
        Cursor cursor = null;
        boolean success = false;
        try {
            writer.deleteAll();
            cursor = API.cursor(plan, queryContext);
            long count = rowIndexer.indexRows(cursor);
            success = true;
            return count;
        }
        finally {
            if (cursor != null)
                cursor.destroy();
            try {
                rowIndexer.close();
                if (success) {
                    writer.commit();
                }
                else {
                    writer.rollback();
                }
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error closing indexer", ex);
            }
        }
    }

    @Override
    public void updateIndex(Session session, IndexName name, Iterable<byte[]> rows)         
    {
        try
        {
            FullTextIndexInfo indexInfo = getIndex(session, name, null);
            StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
            if (adapter == null)
                adapter = store.createAdapter(session, indexInfo.getSchema());
            QueryContext queryContext = new SimpleQueryContext(adapter);
            HKeyCache<com.akiban.qp.row.HKey> cache = new HKeyCache<>(adapter);
            IndexWriter writer = indexInfo.getIndexer().getWriter();
            RowIndexer rowIndexer = new RowIndexer(indexInfo, writer, true);
            Cursor cursor = null;
            boolean success = false;
            try
            {
                Operator operator = indexInfo.getOperator();
                int n = 0;
                for (byte row[] : rows)
                {
                    HKeyRow hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(),
                                                adapter, cache);
                    queryContext.setRow(0, hkeyRow);
                    cursor = API.cursor(operator, queryContext);
                    rowIndexer.updateDocument(cursor, row);
                }
                success = true;
            }
            finally
            {
                rowIndexer.close();
                if (cursor != null)
                    cursor.destroy();

                try
                {
                    if (success)
                        writer.commit();
                    else
                        writer.rollback();
                }
                catch (IOException ex)
                {
                    throw new AkibanInternalException("Error commit writer", ex);
                }
            }
        }
        catch (IOException e)
        {
            throw new AkibanInternalException("Error updating index", e);
        }
    }

    private volatile boolean updateRunning = false;
    private class DefaultUpdateWorker extends TimerTask
    {
        @Override
        public void run()
        {
            runUpdate();
        }
    }; 
    
 
    @Override
    public void schedulePopulate(Session session, IndexName name)
    {   
        logger.error("Scheduled populate {}", name.toString());
        
        try
        {
            // Add the index to the list (in persisit) of indexes to build
            addPopulate(session, name);

            // if there are no scheduled populate workers running, 
            // add one to run shortly. 
            if (populateEnabled && !hasScheduled) {
                 populateTimer.schedule(populateWorker(), populateDelayInterval);
                 hasScheduled = true;
            }
        }
        catch (PersistitException ex)
        {
            throw new AkibanInternalException("Error while scheduling index population", ex);
        }
    }
    
    private final List<? extends BackgroundWork> backgroundWorks
        = Collections.unmodifiableList(Arrays.asList(new BackgroundWorkBase()
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
                                                     },
                                                     new BackgroundWorkBase()
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
                                                     }));

    private static final int populateWork = 0;
    private static final int updateWork = 1;
    
    private volatile boolean populateRunning = false;
    private class DefaultPopulateWorker extends TimerTask
    {
        @Override
        public void run()
        {
            runPopulate();
        }
    };

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


    //----------- private helpers -----------
    protected boolean populateNextIndex(Session session) throws PersistitException 
    {
        IndexName toPopulate = null;
        transactionService.beginTransaction(session);
        Exchange ex = null; 
        try {
            ex = getPopulateExchange(session);
            
            toPopulate = nextInQueue(session, ex, false);
            if (toPopulate != null && stillExists (session, toPopulate)) {
                createIndex(session, toPopulate);
                ex.remove();
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
            if (ex != null) ex.remove();
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
        logger.error("runPopulate started");
        try
        {
            while (populateNextIndex(session)) {}
        }
        catch (PersistitException ex1)
        {
            throw new AkibanInternalException("Error while populating full_text indices", ex1);
        }
        finally
        {
            hasScheduled = false;
            backgroundWorks.get(populateWork).notifyObservers();
            populateRunning = false;
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
        try
        {

            transactionService.beginTransaction(session);
            HKeyBytesStream rows = persistitStore.getChangedRows(session);
            if (rows != null) // if tree is not empty
            {
                do
                {
                    if (populating.get(rows.getIndexName()) == null) {
                        updateIndex(session,
                                    rows.getIndexName(),
                                    rows);
                        rows.remove();
                    }
                }
                while (rows.nextIndex());
                //rows.removeAll(); // done updating. remove all entries
            }
            transactionService.commitTransaction(session);
        }
        catch(PersistitException e)
        {
            throw new AkibanInternalException("Error while maintaning full_text indices");
        }
        finally
        {
            transactionService.rollbackTransactionIfOpen(session);
            session.close();
            backgroundWorks.get(updateWork).notifyObservers();
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
     * @return 
     */
    private boolean forcePopulate()
    {
        // no work to do
        // or it is already being done
        if (!populateEnabled || !hasScheduled || populateRunning)
            return false;

        logger.error("ForcePopulate running");
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

        // get a new timer
        // (So schedulePopulate can schedule new task if new index is created)
        populateTimer = new Timer();
       
        return true;
    }

    private synchronized TimerTask populateWorker ()
    {
        return new DefaultPopulateWorker();
    }

    protected IndexName nextInQueue(Session session, Exchange ex, boolean traversing) throws PersistitException
    {
        Key key = ex.getKey();

        while (ex.next(true))
        {
            key.reset();
            IndexName ret = new IndexName(new TableName(key.decodeString(),
                                                        key.decodeString()),
                                          key.decodeString());
            logger.error("next index in queue: {}", ret.toString());
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
    
    protected Exchange getPopulateExchange(Session session) throws PersistitException
    {
        Exchange ret = treeService.getExchange(session,
                                               treeService.treeLink(POPULATE_SCHEMA,
                                                                    FULL_TEXT_TABLE));
        // start at the first entry
        ret.append(Key.BEFORE);
        return ret;
    }

    private volatile boolean hasScheduled = false;
    private volatile boolean populateEnabled = false;

    private synchronized boolean addPopulate(Session session, IndexName name) throws PersistitException
    {
        Exchange ex = getPopulateExchange(session);

        // Assumption: There is not any existing entry about this index
        // (Because they should have been removed in dropIndex())
        // KEY: schema | table | indexName
        ex.getKey().clear()
                   .append(name.getSchemaName())
                   .append(name.getTableName())
                   .append(name.getName());

        // VALUE: <empty>

        ex.store();
        return true;
    }

    private HKeyRow toHKeyRow(byte rowBytes[], HKeyRowType hKeyRowType,
                              StoreAdapter store, HKeyCache<com.akiban.qp.row.HKey> cache)
    {
        PersistitHKey hkey = (PersistitHKey)store.newHKey(hKeyRowType.hKey());
        Key key = hkey.key();
        key.setEncodedSize(rowBytes.length);
        System.arraycopy(rowBytes, 0, key.getEncodedBytes(), 0, rowBytes.length);
        return new HKeyRow(hKeyRowType, hkey, cache);
    }

}
