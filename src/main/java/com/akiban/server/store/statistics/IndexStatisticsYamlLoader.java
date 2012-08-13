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

package com.akiban.server.store.statistics;

import static com.akiban.server.store.statistics.IndexStatistics.*;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;

import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.util.*;
import java.io.*;

/** Load / dump index stats from / to Yaml files.
 */
public class IndexStatisticsYamlLoader
{
    private AkibanInformationSchema ais;
    private String defaultSchema;

    private final Key key;
    private final PersistitKeyValueSource keySource = new PersistitKeyValueSource();
    private final PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget();
    
    public static final String TABLE_NAME_KEY = "Table";
    public static final String INDEX_NAME_KEY = "Index";
    public static final String TIMESTAMP_KEY = "Timestamp";
    public static final String ROW_COUNT_KEY = "RowCount";
    public static final String SAMPLED_COUNT_KEY = "SampledCount";
    public static final String STATISTICS_COLLECTION_KEY = "Statistics";
    public static final String STATISTICS_COLUMN_COUNT_KEY = "Columns";
    public static final String STATISTICS_HISTOGRAM_COLLECTION_KEY = "Histogram";
    public static final String HISTOGRAM_KEY_ARRAY_KEY = "key";
    public static final String HISTOGRAM_EQUAL_COUNT_KEY = "eq";
    public static final String HISTOGRAM_LESS_COUNT_KEY = "lt";
    public static final String HISTOGRAM_DISTINCT_COUNT_KEY = "distinct";
    
    public static final Comparator<Index> INDEX_NAME_COMPARATOR = 
        new Comparator<Index>() {
            @Override
            public int compare(Index i1, Index i2) {
                return i1.getIndexName().toString().compareTo(i2.getIndexName().toString());
        }
    };

    public IndexStatisticsYamlLoader(AkibanInformationSchema ais, String defaultSchema, KeyCreator keyCreator) {
        this.ais = ais;
        key = keyCreator.createKey();
        this.defaultSchema = defaultSchema;
    }
    
    public Map<Index,IndexStatistics> load(File file, boolean statsIgnoreMissingIndexes) throws IOException {
        Map<Index,IndexStatistics> result = new TreeMap<Index,IndexStatistics>(INDEX_NAME_COMPARATOR);
        Yaml yaml = new Yaml();
        FileInputStream istr = new FileInputStream(file);
        try {
            for (Object doc : yaml.loadAll(istr)) {
                parseStatistics(doc, result, statsIgnoreMissingIndexes);
            }
        }
        finally {
            istr.close();
        }
        return result;
    }

    public Map<Index,IndexStatistics> load(File file) throws IOException {
        return load(file, false);
    }

    protected void parseStatistics(Object obj, Map<Index,IndexStatistics> result, boolean statsIgnoreMissingIndexes) {
        if (!(obj instanceof Map))
            throw new AkibanInternalException("Document not in expected format");
        Map<?,?> map = (Map<?,?>)obj;
        TableName tableName = TableName.create(defaultSchema, 
                                               (String)map.get(TABLE_NAME_KEY));
        Table table = ais.getTable(tableName);
        if (table == null)
            throw new NoSuchTableException(tableName);
        String indexName = (String)map.get(INDEX_NAME_KEY);
        Index index = table.getIndex(indexName);
        if (index == null) {
            index = table.getGroup().getIndex(indexName);
            if (index == null) {
                if (statsIgnoreMissingIndexes)
                    return;
                throw new NoSuchIndexException(indexName);
            }
        }
        IndexStatistics stats = new IndexStatistics(index);
        Date timestamp = (Date)map.get(TIMESTAMP_KEY);
        if (timestamp != null)
            stats.setAnalysisTimestamp(timestamp.getTime());
        Integer rowCount = (Integer)map.get(ROW_COUNT_KEY);
        if (rowCount != null)
            stats.setRowCount(rowCount.longValue());
        Integer sampledCount = (Integer)map.get(SAMPLED_COUNT_KEY);
        if (sampledCount != null)
            stats.setSampledCount(sampledCount.longValue());
        for (Object e : (Iterable)map.get(STATISTICS_COLLECTION_KEY)) {
            Map<?,?> em = (Map<?,?>)e;
            int columnCount = (Integer)em.get(STATISTICS_COLUMN_COUNT_KEY);
            Histogram h = parseHistogram(em.get(STATISTICS_HISTOGRAM_COLLECTION_KEY), 
                                         index, columnCount);
            stats.addHistogram(h);
        }
        result.put(index, stats);
    }

    protected Histogram parseHistogram(Object obj, Index index, int columnCount) {
        if (!(obj instanceof Iterable))
            throw new AkibanInternalException("Histogram not in expected format");
        List<HistogramEntry> entries = new ArrayList<HistogramEntry>();
        for (Object eobj : (Iterable)obj) {
            if (!(eobj instanceof Map))
                throw new AkibanInternalException("Entry not in expected format");
            Map<?,?> emap = (Map<?,?>)eobj;
            Key key = encodeKey(index, columnCount,
                                (List<?>)emap.get(HISTOGRAM_KEY_ARRAY_KEY));
            String keyString = key.toString();
            byte[] keyBytes = new byte[key.getEncodedSize()];
            System.arraycopy(key.getEncodedBytes(), 0, keyBytes, 0, keyBytes.length);
            int eqCount = (Integer)emap.get(HISTOGRAM_EQUAL_COUNT_KEY);
            int ltCount = (Integer)emap.get(HISTOGRAM_LESS_COUNT_KEY);
            int distinctCount = (Integer)emap.get(HISTOGRAM_DISTINCT_COUNT_KEY);
            entries.add(new HistogramEntry(keyString, keyBytes,
                                           eqCount, ltCount, distinctCount));
        }
        return new Histogram(columnCount, entries);
    }

    protected Key encodeKey(Index index, int columnCount, List<?> values) {
        if (values.size() != columnCount)
            throw new AkibanInternalException("Key values do not match column count");
        FromObjectValueSource valueSource = new FromObjectValueSource();
        key.clear();
        keyTarget.attach(key);
        for (int i = 0; i < columnCount; i++) {
            keyTarget.expectingType(index.getKeyColumns().get(i).getColumn());
            valueSource.setReflectively(values.get(i));
            Converters.convert(valueSource, keyTarget);
        }
        return key;
    }

    public void dump(Map<Index,IndexStatistics> stats, Writer writer) throws IOException {
        List<Object> docs = new ArrayList<Object>(stats.size());
        for (Map.Entry<Index,IndexStatistics> stat : stats.entrySet()) {
            docs.add(buildStatistics(stat.getKey(), stat.getValue()));
        }
        DumperOptions dopts = new DumperOptions();
        dopts.setAllowUnicode(false);
        new Yaml(dopts).dumpAll(docs.iterator(), writer);
    }

    protected Object buildStatistics(Index index, IndexStatistics indexStatistics) {
        Map map = new TreeMap();
        map.put(INDEX_NAME_KEY, index.getIndexName().getName());
        map.put(TABLE_NAME_KEY, index.getIndexName().getTableName());
        map.put(TIMESTAMP_KEY, new Date(indexStatistics.getAnalysisTimestamp()));
        map.put(ROW_COUNT_KEY, indexStatistics.getRowCount());
        map.put(SAMPLED_COUNT_KEY, indexStatistics.getSampledCount());
        List<Object> stats = new ArrayList<Object>();
        int nkeys = index.getKeyColumns().size();
        if (index.isSpatial()) nkeys = 1;
        for (int i = 0; i < nkeys; i++) {
            Histogram histogram = indexStatistics.getHistogram(i + 1);
            if (histogram == null) continue;
            stats.add(buildHistogram(index, histogram));
        }
        map.put(STATISTICS_COLLECTION_KEY, stats);
        return map;
    }

    protected Object buildHistogram(Index index, Histogram histogram) {
        Map map = new TreeMap();
        int columnCount = histogram.getColumnCount();
        map.put(STATISTICS_COLUMN_COUNT_KEY, columnCount);
        List<Object> entries = new ArrayList<Object>();
        for (HistogramEntry entry : histogram.getEntries()) {
            Map emap = new TreeMap();
            emap.put(HISTOGRAM_EQUAL_COUNT_KEY, entry.getEqualCount());
            emap.put(HISTOGRAM_LESS_COUNT_KEY, entry.getLessCount());
            emap.put(HISTOGRAM_DISTINCT_COUNT_KEY, entry.getDistinctCount());
            emap.put(HISTOGRAM_KEY_ARRAY_KEY, decodeKey(index, columnCount, 
                                                        entry.getKeyBytes()));
            entries.add(emap);
        }
        map.put(STATISTICS_HISTOGRAM_COLLECTION_KEY, entries);
        return map;
    }

    protected List<Object> decodeKey(Index index, int columnCount, byte[] bytes) {
        key.setEncodedSize(bytes.length);
        System.arraycopy(bytes, 0, key.getEncodedBytes(), 0, bytes.length);
        ToObjectValueTarget valueTarget = new ToObjectValueTarget();
        List<Object> result = new ArrayList<Object>(columnCount);
        for (int i = 0; i < columnCount; i++) {
            if (index.isSpatial()) {
                keySource.attach(key, i, AkType.LONG);
            }
            else {
                keySource.attach(key, index.getKeyColumns().get(i));
            }
            valueTarget.expectType(convertToType(keySource.getConversionType()));
            Converters.convert(keySource, valueTarget);
            result.add(valueTarget.lastConvertedValue());
        }
        return result;
    }

    /** If the AkType's internal representation corresponds to a Java
     * type for which there is standard YAML tag, can use
     * it. Otherwise, must resort to string, either because the
     * internal value isn't friendly (<code>Date</code> is a <code>Long</code>) 
     * or isn't standard (<code>Decimal</code> turns into <code>!!float</code>).
     */
    protected static AkType convertToType(AkType sourceType) {
        switch (sourceType) {
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
        case BOOL:
            return sourceType;
        default:
            return AkType.VARCHAR;
        }
    }

}
