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

import com.foundationdb.qp.operator.CursorLifecycle;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.operator.RowCursorImpl;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.qp.storeadapter.PersistitHKey;
import com.foundationdb.qp.row.HKey;
import com.foundationdb.qp.row.HKeyRow;
import com.foundationdb.qp.row.Row;
import com.foundationdb.qp.rowtype.HKeyRowType;
import com.foundationdb.qp.util.HKeyCache;
import com.foundationdb.server.error.AkibanInternalException;
import com.persistit.Key;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FullTextCursor extends RowCursorImpl
{
    private final QueryContext context;
    private final HKeyRowType rowType;
    private final SearcherManager searcherManager;
    private final Query query;
    private final int limit;
    private final StoreAdapter adapter;
    private IndexSearcher searcher;
    private TopDocs results = null;
    private int position;

    public static final Sort SORT = new Sort(SortField.FIELD_SCORE,
                                             new SortField(IndexedField.KEY_FIELD,
                                                           SortField.Type.STRING));

    private static final Logger logger = LoggerFactory.getLogger(FullTextCursor.class);

    public FullTextCursor(QueryContext context, HKeyRowType rowType, 
                          SearcherManager searcherManager, Query query, int limit) {
        this.context = context;
        this.rowType = rowType;
        this.searcherManager = searcherManager;
        this.query = query;
        this.limit = limit;
        adapter = context.getStore();
        searcher = searcherManager.acquire();
    }

    @Override
    public void open() {
        super.open();
        logger.debug("FullTextCursor: open {}", query);
        if (query == null) {
            setIdle();
        }
        else {
            try {
                results = searcher.search(query, limit, SORT);
            }
            catch (IOException ex) {
                throw new AkibanInternalException("Error searching index", ex);
            }
        }
        position = 0;
    }
    
    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        if (isIdle())
            return null;
        if (position >= results.scoreDocs.length) {
            setIdle();
            results = null;
            return null;
        }
        Document doc;
        try {
            doc = searcher.doc(results.scoreDocs[position++].doc);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error reading document", ex);
        }
        HKey hkey = hkey(doc.get(IndexedField.KEY_FIELD));
        Row row = new HKeyRow(rowType, hkey, new HKeyCache<HKey>(adapter));
        logger.debug("FullTextCursor: yield {}", row);
        return row;
    }

    @Override
    public void close() {
        super.close();
        results = null;
        try {
            searcherManager.release(searcher);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error releasing searcher", ex);
        }
        searcher = null;
    }

    /* Allocate a new <code>PersistitHKey</code> and copy the given
     * key bytes into it. */
    protected HKey hkey(String encoded) {
        PersistitHKey hkey = (PersistitHKey)context.getStore().newHKey(rowType.hKey());
        Key key = hkey.key();
        byte decodedBytes[] = RowIndexer.decodeString(encoded);
        key.setEncodedSize(decodedBytes.length);
        System.arraycopy(decodedBytes, 0, key.getEncodedBytes(), 0, decodedBytes.length);
        return hkey;
    }

}
