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

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.CursorLifecycle;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.persistitadapter.PersistitHKey;
import com.akiban.qp.row.HKey;
import com.akiban.qp.row.HKeyRow;
import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.HKeyRowType;
import com.akiban.qp.util.HKeyCache;
import com.akiban.server.api.dml.ColumnSelector;
import com.akiban.server.error.AkibanInternalException;
import com.persistit.Key;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

public class FullTextCursor implements Cursor
{
    private final QueryContext context;
    private final HKeyRowType rowType;
    private final SearcherManager searcherManager;
    private final Query query;
    private final int limit;
    private final HKeyCache<HKey> hKeyCache;
    private IndexSearcher searcher;
    private TopDocs results;
    private int position;

    public FullTextCursor(QueryContext context, HKeyRowType rowType, 
                          SearcherManager searcherManager, Query query, int limit) {
        this.context = context;
        this.rowType = rowType;
        this.searcherManager = searcherManager;
        this.query = query;
        this.limit = limit;
        hKeyCache = new HKeyCache<>(context.getStore());

        searcher = searcherManager.acquire();
    }

    @Override
    public void open() {
        CursorLifecycle.checkIdle(this);
        try {
            results = searcher.search(query, limit);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error searching index", ex);
        }
        position = 0;
    }
    
    @Override
    public Row next() {
        CursorLifecycle.checkIdleOrActive(this);
        if (results == null)
            return null;
        if (position >= results.scoreDocs.length) {
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
        HKey hkey = hkey(doc.getBinaryValue(IndexedField.KEY_FIELD));
        return new HKeyRow(rowType, hkey, hKeyCache);
    }

    @Override
    public void close() {
        CursorLifecycle.checkIdleOrActive(this);
        results = null;
    }
    
    @Override
    public void destroy() {
        close();
        try {
            searcherManager.release(searcher);
        }
        catch (IOException ex) {
            throw new AkibanInternalException("Error releasing searcher", ex);
        }
        searcher = null;
    }
    
    @Override
    public boolean isIdle()
    {
        return (results == null);
    }
    
    @Override
    public boolean isActive()
    {
        return (results != null);
    }
    
    @Override
    public boolean isDestroyed()
    {
        return (searcher == null);
    }

    @Override
    public void jump(Row row, ColumnSelector columnSelector) {
        throw new UnsupportedOperationException();
    }

    /* Allocate a new <code>PersistitHKey</code> and copy the given
     * key bytes into it. */
    protected HKey hkey(BytesRef keyBytes) {
        PersistitHKey hkey = (PersistitHKey)context.getStore().newHKey(rowType.hKey());
        Key key = hkey.key();
        key.setEncodedSize(keyBytes.length);
        System.arraycopy(keyBytes.bytes, keyBytes.offset, key.getEncodedBytes(), 0, keyBytes.length);
        return hkey;
    }

}
