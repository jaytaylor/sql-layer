/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.Version;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FullTextIndexServiceImpl implements FullTextIndexService, Service {
    public static final String INDEX_PATH_PROPERTY = "akserver.text.indexpath";

    private final ConfigurationService configService;
    private final DXLService dxlService;
    private final Store store;
    private final TransactionService transactionService;
    private final TreeService treeService;

    private File indexPath;
    private Map<String,FullTextIndex> indexes = new HashMap<>();
    private Analyzer analyzer;
    
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
    }

    /* FullTextIndexService */

    @Override
    public void createIndex(Session session, String name, 
                            String schemaName, String tableName,
                            List<String> indexedColumns, boolean populate) {
        FullTextIndex index = new FullTextIndex(name, getIndexPath(),
                                                schemaName, tableName,
                                                indexedColumns);
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        FullTextIndexAIS indexAIS = index.forAIS(ais);
        synchronized (indexes) {
            if (indexes.containsKey(name)) {
                // TODO: Need different exception
                throw new DuplicateIndexException(new com.akiban.ais.model.TableName(schemaName, tableName), name);
            }
            indexes.put(name, index);
        }
        if (populate) {
            try {
                populateIndex(session, indexAIS);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error populating index", ex);
            }
        }
    }

    @Override
    public void dropIndex(Session session, String name) {
        FullTextIndex index;
        synchronized (indexes) {
            index = indexes.remove(name);
        }
        if (index == null) {
            throw new NoSuchIndexException(name);
        }
        for (File f : index.getPath().listFiles()) {
            f.delete();
        }
        index.getPath().delete();
    }

    @Override
    public void populateIndex(Session session, String name) {
        FullTextIndex index;
        synchronized (indexes) {
            index = indexes.get(name);
        }
        if (index == null) {
            throw new NoSuchIndexException(name);
        }
        AkibanInformationSchema ais = dxlService.ddlFunctions().getAIS(session);
        FullTextIndexAIS indexAIS = index.forAIS(ais);
        try {
            populateIndex(session, indexAIS);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error populating index", ex);
        }
    }
    
    @Override
    public List<List<String>> searchIndex(Session session, String name, 
                                          String query, int size) {
        FullTextIndex index;
        synchronized (indexes) {
            index = indexes.get(name);
        }
        if (index == null) {
            throw new NoSuchIndexException(name);
        }
        try {
            return searchIndex(index, query, size);
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
        analyzer = null;
        try {
            for (FullTextIndex index : indexes.values()) {
                Indexer indexer = index.getIndexer();
                if (indexer != null) {
                    indexer.close();
                    index.setIndexer(null);
                }
                Searcher searcher = index.getSearcher();
                if (searcher != null) {
                    searcher.close();
                    index.setSearcher(null);
                }
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
    
    protected synchronized File getIndexPath() {
        if (indexPath == null) {
            indexPath = new File(configService.getProperty(INDEX_PATH_PROPERTY));
        }
        return indexPath;
    }

    protected synchronized Analyzer getAnalyzer() {
        if (analyzer == null) {
            analyzer = analyzer = new StandardAnalyzer(Version.LUCENE_40);
        }
        return analyzer;
    }

    protected void populateIndex(Session session, FullTextIndexAIS indexAIS) 
            throws IOException {
        FullTextIndex index = indexAIS.getIndex();
        Indexer indexer;
        synchronized (index) {
            indexer = index.getIndexer();
            if (indexer == null) {
                indexer = new Indexer(index, getAnalyzer());
            }
            index.setIndexer(indexer);
        }
        Operator plan = indexAIS.fullScan();
        StoreAdapter adapter = session.get(StoreAdapter.STORE_ADAPTER_KEY);
        if (adapter == null)
            adapter = new PersistitAdapter(indexAIS.getSchema(),
                                           store, treeService, 
                                           session, configService);
        QueryContext queryContext = new SimpleQueryContext(adapter);
        RowIndexer rowIndexer = new RowIndexer(indexAIS, indexer.getWriter());
        boolean transaction = false;
        Cursor cursor = null;
        boolean success = false;
        try {
            transactionService.beginTransaction(session);
            transaction = true;
            cursor = API.cursor(plan, queryContext);
            rowIndexer.indexRows(cursor);
            transactionService.commitTransaction(session);
            transaction = false;
            success = true;
        }
        finally {
            if (cursor != null)
                cursor.destroy();
            if (transaction)
                transactionService.rollbackTransaction(session);
            if (!success) {
                rowIndexer.setRollback();
            }
            try {
                rowIndexer.close();
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error closing indexer", ex);
            }
        }
    }

    protected List<List<String>> searchIndex(FullTextIndex index, 
                                             String query, int size) throws IOException {
        Searcher searcher;
        synchronized (index) {
            searcher = index.getSearcher();
            if (searcher == null) {
                searcher = new Searcher(index, getAnalyzer());
            }
            index.setSearcher(searcher);
        }
        return searcher.search(query, size);
    }

}
