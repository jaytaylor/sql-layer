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
    public static final String UPDATE_INTERVAL = "akserver.text.maintenanceInteval";
    public static final String POPULATE_DELAY_INTERVAL = "akserver.text.populateDelayInterval";

    private static final String POPULATE_SCHEMA = "populate";
    private static final String FULL_TEXT_TABLE = "full_text";
    
    private final SessionService sessionService = new SessionServiceImpl();


    private final ConfigurationService configService;
    private final DXLService dxlService;
    private final Store store;
    private final TransactionService transactionService;
    private final TreeService treeService;

    private File indexPath;

    private final Timer maintenanceTimer = new Timer();
    private final long maintenanceInterval;
    private final TimerTask updateWorker;
    private final PersistitStore persistitStore;

    private final Timer populateTimer = new Timer();
    private final long populateDelayInterval;
    private final TimerTask populateWorker;

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
        persistitStore = store.getPersistitStore();

        maintenanceInterval = Long.parseLong(configService.getProperty(UPDATE_INTERVAL));
        updateWorker = DEFAULT_UPDATE_WORKER;
        maintenanceTimer.scheduleAtFixedRate(updateWorker, maintenanceInterval, maintenanceInterval);
        
        populateDelayInterval = Long.parseLong(configService.getProperty(POPULATE_DELAY_INTERVAL));
        populateWorker = DEFAULT_POPULATE_WORKER;

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
                writer.deleteAll();
                transactionService.beginTransaction(session);
                transaction = true;
                for (byte row[] : rows)
                {
                    HKeyRow hkeyRow = toHKeyRow(row, indexInfo.getHKeyRowType(),
                                                adapter, cache);
                    queryContext.setRow(0, hkeyRow);
                    cursor = API.cursor(indexInfo.getOperator(hkeyRow), queryContext);
                    rowIndexer.updateDocument(cursor, row);
                }
                transaction = false;
                success = true;
            }
            finally
            {
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
                transactionService.beginTransaction(session);
                HKeyBytesStream rows = persistitStore.getChangedRows(session);
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
                transactionService.commitTransaction(session);
            }
            catch(PersistitException e)
            {
                throw new AkibanInternalException("Error while maintaning full_text indices");
            }
            finally
            {
                transactionService.rollbackTransaction(session);
                session.close();
            }
        }
    }; 
    
 
    @Override
    public void schedulePopulate(IndexName name)
    {
        try
        {
            addChange(sessionService.createSession(), name);
            populateTimer.schedule(populateWorker, populateDelayInterval);
        }
        catch (PersistitException ex)
        {
            throw new AkibanInternalException("Error while scheduling index population", ex);
        }
    }
    
    private final TimerTask DEFAULT_POPULATE_WORKER = new TimerTask()
    {
        @Override
        public synchronized void run()
        {
            try
            {
                Session session = sessionService.createSession();
                Exchange ex = getPopulateExchange(session);
                
                IndexName toPopulate;
                //createIndex(Session session, IndexName name)
                while ((toPopulate = nextInQueue(ex)) != null)
                {
                    createIndex(session, toPopulate);
                    ex.fetchAndRemove();
                }
                
                // All indices have been populated
                // so cancel future task(s) now
                populateTimer.cancel();
            }
            catch (PersistitException ex1)
            {
                throw new AkibanInternalException("Error while populating full_text indices", ex1);
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
    
    void enableUpdateWorker()
    {
        maintenanceTimer.scheduleAtFixedRate(updateWorker, maintenanceInterval, maintenanceInterval);
    }
    
    void disablePopulateWorker()
    {
        populateWorker.cancel();
        populateTimer.cancel();
        populateTimer.purge();
    }
    
    void enablePopulateWorker()
    {
        // TODO:
        //populateWorker.run();
    }
    
    //----------- private helpers -----------
    private IndexName nextInQueue(Exchange ex) throws PersistitException
    {
        Key key = ex.getKey();
        if (key == null) // empty tree?
            return null;
        else
        {
            IndexName ret = new IndexName(new TableName(key.decodeString(),
                                                        key.decodeString()),
                                          key.decodeString());
            ex.next();
            return ret;
        }
    }
    
    private Exchange getPopulateExchange(Session session)
    {
        return treeService.getExchange(session,
                                              treeService.treeLink(POPULATE_SCHEMA,
                                                                   FULL_TEXT_TABLE));
    }
    
    private Exchange nextPopulateEntry(Session session, IndexName indexName) throws PersistitException
    {   
        Exchange ret = getPopulateExchange(session);
        do
        {
            Key key = ret.getKey();
            String schema = key.decodeString();
            String table = key.decodeString();
            String index = key.decodeString();
            
            if (schema.equals(indexName.getSchemaName())
                    && table.equals(indexName.getTableName())
                    && index.equals(indexName.getName()))
                return null;
        }
        while (ret.next());

        return ret;
    }

    private void addChange(Session session,
                           IndexName indexName) throws PersistitException
    {
        Exchange ex = nextPopulateEntry(session, indexName);

        // 'promise' for populating this index already exists
        if (ex == null)
            return;
        
        // KEY: schema | table | indexName
        ex.clear().append(indexName.getSchemaName())
                  .append(indexName.getTableName())
                  .append(indexName.getName());

        // VALUE: <empty>
    }
    
    private HKeyRow toHKeyRow(byte rowBytes[], HKeyRowType hKeyRowType,
                              StoreAdapter store, HKeyCache<com.akiban.qp.row.HKey> cache )
    {
        PersistitHKey hkey = (PersistitHKey)store.newHKey(hKeyRowType.hKey());
        Key key = hkey.key();
        key.setEncodedSize(rowBytes.length);
        System.arraycopy(rowBytes, 0, key.getEncodedBytes(), 0, rowBytes.length);
        return new HKeyRow(hKeyRowType, hkey, cache);
    }

}
