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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.*;
import java.util.List;
import java.util.Set;

public class FullTextIndex implements CacheValueGenerator<FullTextIndexAIS>
{
    private final String name;
    private final File path;
    private final String schemaName, tableName;
    private final List<String> indexedColumns;
    private String defaultFieldName;
    private List<String> keyColumns;
    private Set<String> casePreservingFieldNames;
    private Directory directory;
    private Analyzer analyzer;
    private Indexer indexer;
    private Searcher searcher;

    public FullTextIndex(String name, File basepath,
                         String schemaName, String tableName,
                         List<String> indexedColumns) {
        this.name = name;
        this.path = new File(basepath, name);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.indexedColumns = indexedColumns;
    }

    public String getName() {
        return name;
    }

    public File getPath() {
        return path;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getIndexedColumns() {
        return indexedColumns;
    }

    public List<String> getKeyColumns() {
        return keyColumns;
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

    public synchronized void close() throws IOException {
        if (directory != null) {
            directory.close();
            directory = null;
        }
    }

    public FullTextIndexAIS forAIS(AkibanInformationSchema ais) {
        return ais.getCachedValue(this, this);
    }

    @Override
    public FullTextIndexAIS valueFor(AkibanInformationSchema ais) {
        FullTextIndexAIS result = new FullTextIndexAIS(this, ais);
        result.init();
        if (keyColumns == null)
            keyColumns = result.getKeyColumns();
        if (casePreservingFieldNames == null)
            casePreservingFieldNames = result.getCasePreservingFieldNames();
        if (defaultFieldName == null)
            defaultFieldName = result.getDefaultFieldName();
        return result;
    }

    public Analyzer getAnalyzer() {
        return analyzer;
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
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
