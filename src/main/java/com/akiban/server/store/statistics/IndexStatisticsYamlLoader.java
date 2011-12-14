/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.store.statistics;

import static com.akiban.server.store.statistics.IndexStatistics.*;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;

import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;
import com.persistit.Persistit;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchIndexException;
import com.akiban.server.error.NoSuchTableException;

import org.yaml.snakeyaml.Yaml;

import java.util.*;
import java.io.*;

/** Load / dump index stats from / to Yaml files.
 */
public class IndexStatisticsYamlLoader
{
    private AkibanInformationSchema ais;
    private String defaultSchema;

    private final Key key = new Key((Persistit)null);
    private final PersistitKeyValueSource keySource = new PersistitKeyValueSource();
    private final PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget();
    
    public static final String TABLE_NAME_KEY = "Table";
    public static final String INDEX_NAME_KEY = "Index";
    public static final String STATISTICS_COLLECTION_KEY = "Statistics";
    public static final String STATISTICS_COLUMN_COUNT_KEY = "Columns";
    public static final String STATISTICS_HISTOGRAM_COLLECTION_KEY = "Histogram";
    public static final String HISTOGRAM_KEY_ARRAY_KEY = "key";
    public static final String HISTOGRAM_EQUAL_COUNT_KEY = "eq";
    public static final String HISTOGRAM_LESS_COUNT_KEY = "lt";
    public static final String HISTOGRAM_DISTINCT_COUNT_KEY = "distinct";
    
    public IndexStatisticsYamlLoader(AkibanInformationSchema ais, String defaultSchema) {
        this.ais = ais;
        this.defaultSchema = defaultSchema;
    }
    
    public Map<Index,IndexStatistics> load(File file) throws IOException {
        Map<Index,IndexStatistics> result = new HashMap<Index,IndexStatistics>();
        Yaml yaml = new Yaml();
        FileInputStream istr = new FileInputStream(file);
        try {
            for (Object doc : yaml.loadAll(istr)) {
                IndexStatistics indexStatistics = parseStatistics(doc);
                result.put(indexStatistics.getIndex(), indexStatistics);
            }
        }
        finally {
            istr.close();
        }
        return result;
    }

    protected IndexStatistics parseStatistics(Object obj) {
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
            if (index == null)
                throw new NoSuchIndexException(indexName);
        }
        IndexStatistics result = new IndexStatistics(index);
        for (Object e : (Iterable)map.get(STATISTICS_COLLECTION_KEY)) {
            Map<?,?> em = (Map<?,?>)e;
            int columnCount = (Integer)em.get(STATISTICS_COLUMN_COUNT_KEY);
            Histogram h = parseHistogram(em.get(STATISTICS_HISTOGRAM_COLLECTION_KEY), 
                                         index, columnCount);
            result.addHistogram(h);
        }
        return result;
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
        return new Histogram(index, columnCount, entries);
    }

    protected Key encodeKey(Index index, int columnCount, List<?> values) {
        if (values.size() != columnCount)
            throw new AkibanInternalException("Key values do not match column count");
        FromObjectValueSource valueSource = new FromObjectValueSource();
        key.clear();
        keyTarget.attach(key);
        for (int i = 0; i < columnCount; i++) {
            keyTarget.expectingType(index.getColumns().get(i).getColumn().getType().akType());
            valueSource.setReflectively(values.get(i));
            Converters.convert(valueSource, keyTarget);
        }
        return key;
    }

    public void dump(Collection<IndexStatistics> stats, File file) throws IOException {
        List<Object> docs = new ArrayList<Object>(stats.size());
        for (IndexStatistics stat : stats) {
            docs.add(buildStatistics(stat));
        }
        Yaml yaml = new Yaml();
        FileWriter ostr = new FileWriter(file);
        try {
            yaml.dumpAll(docs.iterator(), ostr);
        }
        finally {
            ostr.close();
        }
    }

    protected Object buildStatistics(IndexStatistics indexStatistics) {
        Map map = new TreeMap();
        Index index = indexStatistics.getIndex();
        map.put(INDEX_NAME_KEY, index.getIndexName().getName());
        map.put(TABLE_NAME_KEY, index.getIndexName().getTableName());
        List<Object> stats = new ArrayList<Object>();
        for (int i = 0; i < index.getColumns().size(); i++) {
            Histogram histogram = indexStatistics.getHistogram(i + 1);
            if (histogram == null) continue;
            stats.add(buildHistogram(histogram));
        }
        map.put(STATISTICS_COLLECTION_KEY, stats);
        return map;
    }

    protected Object buildHistogram(Histogram histogram) {
        Map map = new TreeMap();
        Index index = histogram.getIndex();
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
            keySource.attach(key, index.getColumns().get(i));
            // TODO: Special handling for date/time types to make them
            // more legible than internal long representation?
            result.add(valueTarget.convertFromSource(keySource));
        }
        return result;
    }

}
