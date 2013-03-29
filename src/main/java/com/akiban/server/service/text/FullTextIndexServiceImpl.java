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
import com.akiban.ais.model.IndexName;
import com.akiban.ais.model.TableName;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.error.AkibanInternalException;
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

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;

import com.google.inject.Inject;
import com.persistit.Exchange;
import com.persistit.exception.PersistitException;
import com.persistit.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Timer;
import java.util.TimerTask;


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

    private final Timer maintenanceTimer = new Timer();
    private long maintenanceInterval;
    private TimerTask updateWorker;
    private PersistitStore persistitStore;

    private final Timer populateTimer = new Timer();
    private long populateDelayInterval;

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

        updateWorker = DEFAULT_UPDATE_WORKER;

    }

    /* FullTextIndexService */

    @Override
    public long createIndex(Session session, IndexName name) {
        if (session == null)
            session = sessionService.createSession();
        FullTextIndexInfo index = getIndex(session, name);
        try {
            return populateIndex(session, index);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error populating index", ex);
        }
    }

    @Override
    public void dropIndex(Session session, IndexName name) {
        FullTextIndexInfo index = getIndex(session, name);
        index.deletePath();
        synchronized (indexes) {
            indexes.remove(name);
        }
    }

    @Override
    public Cursor searchIndex(QueryContext context, IndexName name, Query query, int limit) {
        FullTextIndexInfo index = getIndex(context.getSession(), name);
        try {
            return index.getSearcher().search(context, index.getHKeyRowType(), 
                                              query, limit);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error searching index", ex);
        }
    }

    /* Service */
    
    @Override
    public void start() {
        persistitStore = store.getPersistitStore();
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
            adapter = new PersistitAdapter(index.getSchema(),
                                           store, treeService, 
                                           session, configService);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        IndexWriter writer = indexer.getWriter();
        RowIndexer rowIndexer = new RowIndexer(index, writer, false);
        boolean transaction = false;
        Cursor cursor = null;
        boolean success = false;
        try {
            writer.deleteAll();
            transactionService.beginTransaction(session);
            transaction = true;
            cursor = API.cursor(plan, queryContext);
            long count = rowIndexer.indexRows(cursor);
            transactionService.commitTransaction(session);
            transaction = false;
            success = true;
            return count;
        }
        finally {
            if (cursor != null)
                cursor.destroy();
            if (transaction)
                transactionService.rollbackTransaction(session);
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
            FullTextIndexInfo indexInfo = getIndex(session, name);
            StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
            if (adapter == null)
                adapter = new PersistitAdapter(indexInfo.getSchema(),
                                               store, treeService, 
                                               session, configService);
            QueryContext queryContext = new SimpleQueryContext(adapter);
            HKeyCache<com.akiban.qp.row.HKey> cache = new HKeyCache<>(adapter);
            IndexWriter writer = indexInfo.getIndexer().getWriter();
            RowIndexer rowIndexer = new RowIndexer(indexInfo, writer, false);
            boolean transaction = false;
            Cursor cursor = null;
            boolean success = false;
            try
            {
                transactionService.beginTransaction(session);
                transaction = true;
                Operator operator = indexInfo.getOperator();
                for (byte row[] : rows)
                {
                    HKeyRow hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(),
                                                adapter, cache);
                    queryContext.setRow(0, hkeyRow);
                    cursor = API.cursor(operator, queryContext);
                    rowIndexer.updateDocument(cursor, row);
                }
                transaction = false;
                success = true;
            }
            finally
            {
                rowIndexer.close();
                if (cursor != null)
                    cursor.destroy();

                if (transaction)
                    transactionService.rollbackTransaction(session);

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

    private final TimerTask DEFAULT_UPDATE_WORKER = new TimerTask()
    {
        @Override
        // 'sync' because only one worker can work at a time?
        public synchronized void run()
        {
            Session session = sessionService.createSession();
            try
            {
                HKeyBytesStream rows = persistitStore.getChangedRows(session);
                if (rows != null)
                    do
                    {
                        // do the update
                        updateIndex(session,
                                    rows.getIndexName(),
                                    rows);

                        // remove the entries (relating to this index)
                        // that have been taken care of
                        rows.removeAll();
                    }
                    while (rows.nextIndex());
            }
            catch(PersistitException e)
            {
                throw new AkibanInternalException("Error while maintaning full_text indices");
            }
            finally
            {
                session.close();
            }
        }
    }; 
    
 
    @Override
    public void schedulePopulate(String schema, String table, String index)
    {
        Session session = sessionService.createSession();
        boolean success = false;
        try
        {
            transactionService.beginTransaction(session);
            if(addPopulate(session, schema, table, index) && !hasScheduled)
            {
                populateTimer.schedule(populateWorker(), populateDelayInterval);
                hasScheduled = true;
            }

            success = true;            
        }
        catch (PersistitException ex)
        {
            throw new AkibanInternalException("Error while scheduling index population", ex);
        }
        finally
        {
            if (success)
                transactionService.commitTransaction(session);
            else
                transactionService.rollbackTransaction(session);
            session.close();
        }
    }
    
    private class DefaultPopulateWorker extends TimerTask
    {
        @Override
        public synchronized void run()
        {
            Session session = sessionService.createSession();
            try
            {
                Exchange ex = getPopulateExchange(session);
                IndexName toPopulate;
                while ((toPopulate = nextInQueue(ex)) != null)
                {
                    createIndex(session, toPopulate);
                    ex.fetchAndRemove();
                }

                hasScheduled = false;
            }
            catch (PersistitException ex1)
            {
                throw new AkibanInternalException("Error while populating full_text indices", ex1);
            }
            finally
            {
                session.close();
            }
        }
        
    };

    // ---------- for testing ---------------
    void disableUpdateWorker()
    {
        updateWorker.cancel();
        maintenanceTimer.cancel();
        maintenanceTimer.purge();
        
    }
    
    protected void enableUpdateWorker()
    {
        maintenanceInterval = Long.parseLong(configService.getProperty(UPDATE_INTERVAL));
        maintenanceTimer.scheduleAtFixedRate(updateWorker, maintenanceInterval, maintenanceInterval);
    }
    
    protected void enablePopulateWorker()
    {
        populateDelayInterval = Long.parseLong(configService.getProperty(POPULATE_DELAY_INTERVAL));
        populateTimer.schedule(populateWorker(), populateDelayInterval);
        hasScheduled = true;
    }
    
    void disablePopulateWorker()
    {
        populateTimer.cancel();
        populateTimer.purge();
        hasScheduled = false;
    }
    
    private TimerTask populateWorker()
    {
        return new DefaultPopulateWorker();
    }

    //----------- private helpers -----------
    private IndexName nextInQueue(Exchange ex) throws PersistitException
    {
        Key key = ex.getKey();

        if (ex.next(true)) // empty tree?
        {
            IndexName ret = new IndexName(new TableName(key.decodeString(),
                                                        key.decodeString()),
                                          key.decodeString());
            return ret;
        }
        else
            return null;
    }
    
    private Exchange getPopulateExchange(Session session) throws PersistitException
    {
        Exchange ret = treeService.getExchange(session,
                                               treeService.treeLink(POPULATE_SCHEMA,
                                                                    FULL_TEXT_TABLE));
        // start at the first entry
        ret.append(Key.BEFORE);
        return ret;
    }
    
    private synchronized Exchange nextPopulateEntry(Session session,
                                       String schema,
                                       String table,
                                       String index) throws PersistitException
    {   
        Exchange ret = getPopulateExchange(session);

        // see if the entry for this index already exists
        ret.clear().append(schema).append(table).append(index);
        if (ret.traverse(Key.Direction.EQ, true, 0))
            return null;
        else
            return ret;
    }

    private volatile boolean hasScheduled = false;
    private synchronized boolean addPopulate(Session session,
                           String schema,
                           String table,
                           String index) throws PersistitException
    {
        Exchange ex = nextPopulateEntry(session, schema, table, index);

        // 'promise' for populating this index already exists
        if (ex == null)
            return false;
        
        // KEY: schema | table | indexName
        ex.getKey().clear()
                   .append(schema)
                   .append(table)
                   .append(index);

        
        // VALUE: <empty>

        ex.store();
        return true;
    }
    
//    private Row constructRow(byte hkeyBytes[], Group group, Session session)
//    {
//        
//    }
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
