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

import com.foundationdb.qp.operator.RowCursor;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.HKeyRowType;

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

    public RowCursor search(QueryContext context, HKeyRowType rowType,
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
