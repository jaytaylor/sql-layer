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
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.operator.SimpleQueryContext;
import com.akiban.qp.operator.StoreAdapter;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.persistitadapter.PersistitAdapter;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.DuplicateIndexException;
import com.akiban.server.service.Service;
import com.akiban.server.service.config.ConfigurationService;
import com.akiban.server.service.dxl.DXLService;
import com.akiban.server.service.session.Session;
import com.akiban.server.service.transaction.TransactionService;
import com.akiban.server.service.tree.TreeService;
import com.akiban.server.store.Store;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Query;

import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class FullTextIndexServiceImpl extends FullTextIndexInfosImpl implements FullTextIndexService, Service {
    public static final String INDEX_PATH_PROPERTY = "akserver.text.indexpath";

    private final ConfigurationService configService;
    private final DXLService dxlService;
    private final Store store;
    private final TransactionService transactionService;
    private final TreeService treeService;

    private File indexPath;
    
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
        store.setFullTextService(this);
    }

    /* FullTextIndexService */

    @Override
    public long createIndex(Session session, IndexName name) {
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

}
