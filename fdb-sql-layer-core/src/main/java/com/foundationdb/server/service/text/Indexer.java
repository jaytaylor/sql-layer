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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;
import org.apache.lucene.index.IndexReader;

public class Indexer implements Closeable
{
    private final FullTextIndexShared index;
    private final IndexWriter writer;
    
    public Indexer(FullTextIndexShared index, Analyzer analyzer) throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        iwc.setMaxBufferedDeleteTerms(1); // The deletion needs to be reflected immediately (on disk)
        this.index = index;
        this.writer = new IndexWriter(index.open(),  iwc);

    }

    public FullTextIndexShared getIndex() {
        return index;
    }

    public IndexWriter getWriter() {
        return writer;
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
