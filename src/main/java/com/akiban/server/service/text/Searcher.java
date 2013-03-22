
package com.akiban.server.service.text;

import com.akiban.qp.operator.Cursor;
import com.akiban.qp.operator.QueryContext;
import com.akiban.qp.rowtype.HKeyRowType;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Searcher implements Closeable
{
    public static final int DEFAULT_LIMIT = Integer.MAX_VALUE;

    private final FullTextIndexShared index;
    private final SearcherManager searcherManager;

    public Searcher(FullTextIndexShared index, Analyzer analyzer) throws IOException {
        this.index = index;
        this.searcherManager = new SearcherManager(index.open(), new SearcherFactory());
    }

    public Cursor search(QueryContext context, HKeyRowType rowType,
                         Query query, int limit)
            throws IOException {
        searcherManager.maybeRefresh(); // TODO: Move to better place.
        if (limit <= 0) limit = DEFAULT_LIMIT;
        return new FullTextCursor(context, rowType, searcherManager, query, limit);
    }

    @Override
    public void close() throws IOException {
        searcherManager.close();
    }

}
