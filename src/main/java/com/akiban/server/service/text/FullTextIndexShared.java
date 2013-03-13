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
