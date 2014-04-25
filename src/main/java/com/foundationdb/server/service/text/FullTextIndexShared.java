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

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.ais.model.IndexName;
import com.foundationdb.server.store.format.FullTextIndexFileStorageDescription;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.standard.StandardQueryParser;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
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
        FullTextIndexFileStorageDescription storage = (FullTextIndexFileStorageDescription)info.getIndex().getStorageDescription();
        path = storage.mergePath(basepath);
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
