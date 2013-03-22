
package com.akiban.server.service.text;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.util.Version;

import java.io.Closeable;
import java.io.IOException;

public class Indexer implements Closeable
{
    private final FullTextIndexShared index;
    private final IndexWriter writer;
    
    public Indexer(FullTextIndexShared index, Analyzer analyzer) throws IOException {
        this.index = index;
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_40, analyzer);
        this.writer = new IndexWriter(index.open(), iwc);
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
