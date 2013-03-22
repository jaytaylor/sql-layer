
package com.akiban.server.service.text;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;
import com.akiban.ais.model.IndexName;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.util.List;
import java.util.Set;

public class FullTextIndexShared implements CacheValueGenerator<FullTextIndexInfo>, Closeable
{
    private final IndexName name;
    private File path;
    private Set<String> casePreservingFieldNames;
    private String defaultFieldName;
    private Directory directory;
    private Analyzer analyzer;
    private StandardQueryParser parser;
    private Indexer indexer;
    private Searcher searcher;

    public FullTextIndexShared(IndexName name) {
        this.name = name;
    }

    public FullTextIndexInfo init(AkibanInformationSchema ais, final FullTextIndexInfo info, 
                                  File basepath) {
        path = new File(basepath, info.getIndex().getTreeName());
        casePreservingFieldNames = info.getCasePreservingFieldNames();
        defaultFieldName = info.getDefaultFieldName();
        // Put into cache.
        return ais.getCachedValue(this, new CacheValueGenerator<FullTextIndexInfo>() {
                                      @Override
                                      public FullTextIndexInfo valueFor(AkibanInformationSchema ais) {
                                          return info;
                                      }
                                  });
    }

    public IndexName getName() {
        return name;
    }

    public File getPath() {
        return path;
    }

    public Set<String> getCasePreservingFieldNames() {
        return casePreservingFieldNames;
    }

    public String getDefaultFieldName() {
        return defaultFieldName;
    }

    public synchronized Directory open() throws IOException {
        if (directory == null) {
            directory = FSDirectory.open(path);
        }
        return directory;
    }

    @Override
    public synchronized void close() throws IOException {
        if (indexer != null) {
            indexer.close();
            indexer = null;
        }
        if (searcher != null) {
            searcher.close();
            searcher = null;
        }
        if (directory != null) {
            directory.close();
            directory = null;
        }
    }

    public FullTextIndexInfo forAIS(AkibanInformationSchema ais) {
        return ais.getCachedValue(this, this);
    }

    @Override
    public FullTextIndexInfo valueFor(AkibanInformationSchema ais) {
        FullTextIndexInfo result = new FullTextIndexInfo(this);
        result.init(ais);
        return result;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public StandardQueryParser getParser() {
        return parser;
    }

    public void setParser(StandardQueryParser parser) {
        this.parser = parser;
    }

    public Indexer getIndexer() {
        return indexer;
    }

    public void setIndexer(Indexer indexer) {
        this.indexer = indexer;
    }

    public Searcher getSearcher() {
        return searcher;
    }

    public void setSearcher(Searcher searcher) {
        this.searcher = searcher;
    }

}
