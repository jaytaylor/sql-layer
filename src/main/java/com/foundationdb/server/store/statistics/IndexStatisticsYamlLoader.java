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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.TableName;

import com.foundationdb.server.PersistitKeyValueSource;
import com.foundationdb.server.PersistitKeyValueTarget;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.aktypes.AkBool;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.value.Value;
import com.foundationdb.server.types.value.ValueSources;
import com.foundationdb.util.AkibanAppender;
import com.persistit.Key;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.NoSuchIndexException;
import com.foundationdb.server.error.NoSuchTableException;

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
    
    public static final String TABLE_NAME_KEY = "Table";
    public static final String INDEX_NAME_KEY = "Index";
    public static final String TIMESTAMP_KEY = "Timestamp";
    public static final String ROW_COUNT_KEY = "RowCount";
    public static final String SAMPLED_COUNT_KEY = "SampledCount";
    public static final String STATISTICS_COLLECTION_KEY = "Statistics";
    public static final String STATISTICS_COLUMN_COUNT_KEY = "Columns";
    public static final String STATISTICS_COLUMN_FIRST_COLUMN_KEY = "FirstColumn";
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
        this.defaultSchema = defaultSchema;
        key = keyCreator.createKey();
    }
    
    public Map<Index,IndexStatistics> load(File file, boolean statsIgnoreMissingIndexes) throws IOException {
        Map<Index,IndexStatistics> result = new TreeMap<>(INDEX_NAME_COMPARATOR);
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
            Integer firstColumn = (Integer)em.get(STATISTICS_COLUMN_FIRST_COLUMN_KEY);
            Histogram h = parseHistogram(em.get(STATISTICS_HISTOGRAM_COLLECTION_KEY),
                                         index, firstColumn == null ? 0 : firstColumn, columnCount);
            stats.addHistogram(h);
        }
        result.put(index, stats);
    }

    protected Histogram parseHistogram(Object obj, Index index, int firstColumn, int columnCount) {
        if (!(obj instanceof Iterable))
            throw new AkibanInternalException("Histogram not in expected format");
        List<HistogramEntry> entries = new ArrayList<>();
        for (Object eobj : (Iterable)obj) {
            if (!(eobj instanceof Map))
                throw new AkibanInternalException("Entry not in expected format");
            Map<?,?> emap = (Map<?,?>)eobj;
            Key key = encodeKey(index, firstColumn, columnCount,
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
        return new Histogram(firstColumn, columnCount, entries);
    }

    protected Key encodeKey(Index index, int firstColumn, int columnCount, List<?> values) {
        if (values.size() != columnCount)
            throw new AkibanInternalException("Key values do not match column count");
        int firstSpatialColumn = Integer.MAX_VALUE;
        if (index.isSpatial()) {
            firstSpatialColumn = index.firstSpatialArgument();
        }
        key.clear();
        PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget(index.getIndexName());
        keyTarget.attach(key);
        for (int i = 0; i < columnCount; i++) {
            Object value = values.get(i);
            if (value instanceof byte[]) {
                appendRawSegment((byte[])value);
                continue;
            }
            TInstance type;
            if (i == firstSpatialColumn) {
                type = MNumeric.BIGINT.instance(true);
            }
            else {
                int offset = i;
                if (i > firstSpatialColumn) {
                    offset += index.spatialColumns() - 1;
                }
                Column column = index.getKeyColumns().get(firstColumn + offset).getColumn();
                type = column.getType();
                column.getCollator();
            }
            // For example, for DECIMAL, value will be a
            // String, pvalue will be a its VARCHAR, and pvalue2
            // will be a BigDecimalWrapper, which only
            // TBigDecimal.writeCollating knows how to unwrap into
            // a Key.
            
            TPreptimeValue pvalue = null;
            if (value == null)
                pvalue = ValueSources.fromObject(value, type);
            else
                pvalue = ValueSources.fromObject(value, (TInstance) null);
            TExecutionContext context = new TExecutionContext(null,
                                                              Collections.singletonList(pvalue.type()),
                    type,
                                                              null, null, null, null);
            Value pvalue2 = new Value(type);
            type.typeClass().fromObject(context, pvalue.value(), pvalue2);
            type.writeCollating(pvalue2, keyTarget);
        }
        return key;
    }

    protected void appendRawSegment(byte[] encoded) {
        assert (findNul(encoded) == encoded.length - 1) : Arrays.toString(encoded);
        int size = key.getEncodedSize();
        key.setEncodedSize(size + encoded.length);
        System.arraycopy(encoded, 0, key.getEncodedBytes(), size, encoded.length);
    }

    protected static int findNul(byte[] encoded) {
        for (int i = 0; i < encoded.length; i++) {
            if (encoded[i] == 0) {
                return i;
            }
        }
        return -1;
    }

    public void dump(Map<Index,IndexStatistics> stats, Writer writer) throws IOException {
        List<Object> docs = new ArrayList<>(stats.size());
        for (Map.Entry<Index,IndexStatistics> stat : stats.entrySet()) {
            docs.add(buildStatistics(stat.getKey(), stat.getValue()));
        }
        DumperOptions dopts = new DumperOptions();
        dopts.setAllowUnicode(false);
        new Yaml(dopts).dumpAll(docs.iterator(), writer);
    }

    protected Object buildStatistics(Index index, IndexStatistics indexStatistics) {
        Map<String, Object> map = new TreeMap<>();
        map.put(INDEX_NAME_KEY, index.getIndexName().getName());
        map.put(TABLE_NAME_KEY, index.getIndexName().getTableName());
        map.put(TIMESTAMP_KEY, new Date(indexStatistics.getAnalysisTimestamp()));
        map.put(ROW_COUNT_KEY, indexStatistics.getRowCount());
        map.put(SAMPLED_COUNT_KEY, indexStatistics.getSampledCount());
        List<Object> stats = new ArrayList<>();
        int nkeys = index.getKeyColumns().size();
        if (index.isSpatial()) nkeys -= index.spatialColumns() - 1;
        // Multi-column histograms
        for (int i = 0; i < nkeys; i++) {
            Histogram histogram = indexStatistics.getHistogram(0, i + 1);
            if (histogram == null) continue;
            stats.add(buildHistogram(index, histogram));
        }
        // Single-column histograms
        for (int i = 1; i < nkeys; i++) {
            Histogram histogram = indexStatistics.getHistogram(i, 1);
            if (histogram == null) continue;
            stats.add(buildHistogram(index, histogram));
        }
        map.put(STATISTICS_COLLECTION_KEY, stats);
        return map;
    }

    protected Object buildHistogram(Index index, Histogram histogram) {
        Map<String, Object> map = new TreeMap<>();
        int columnCount = histogram.getColumnCount();
        int firstColumn = histogram.getFirstColumn();
        map.put(STATISTICS_COLUMN_COUNT_KEY, columnCount);
        map.put(STATISTICS_COLUMN_FIRST_COLUMN_KEY, firstColumn);
        List<Object> entries = new ArrayList<>();
        for (HistogramEntry entry : histogram.getEntries()) {
            Map<String, Object> emap = new TreeMap<>();
            emap.put(HISTOGRAM_EQUAL_COUNT_KEY, entry.getEqualCount());
            emap.put(HISTOGRAM_LESS_COUNT_KEY, entry.getLessCount());
            emap.put(HISTOGRAM_DISTINCT_COUNT_KEY, entry.getDistinctCount());
            emap.put(HISTOGRAM_KEY_ARRAY_KEY, decodeKey(index, firstColumn, columnCount, entry.getKeyBytes()));
            entries.add(emap);
        }
        map.put(STATISTICS_HISTOGRAM_COLLECTION_KEY, entries);
        return map;
    }

    protected List<Object> decodeKey(Index index, int firstColumn, int columnCount, byte[] bytes) {
        key.setEncodedSize(bytes.length);
        System.arraycopy(bytes, 0, key.getEncodedBytes(), 0, bytes.length);
        int firstSpatialColumn = Integer.MAX_VALUE;
        if (index.isSpatial()) {
            firstSpatialColumn = index.firstSpatialArgument();
        }
        List<Object> result = new ArrayList<>(columnCount);

        for (int i = 0; i < columnCount; i++) {
            TInstance type;
            boolean useRawSegment;
            if (i == firstSpatialColumn) {
                type = MNumeric.BIGINT.instance(true);
                useRawSegment = false;
            }
            else {
                int offset = i;
                if (i > firstSpatialColumn) {
                    offset += index.spatialColumns() - 1;
                }
                Column column = index.getKeyColumns().get(firstColumn + offset).getColumn();
                type = column.getType();
                AkCollator collator = column.getCollator();
                useRawSegment = ((collator != null) && !collator.isRecoverable());
            }
            Object keyValue;
            if (useRawSegment) {
                keyValue = getRawSegment(key, i);
            }
            else {
                PersistitKeyValueSource keySource = new PersistitKeyValueSource(type);
                keySource.attach(key, i, type);
                if (convertToType(type)) {
                    keyValue = ValueSources.toObject(keySource);
                }
                else if (keySource.isNull()) {
                    keyValue = null;
                }
                else {
                    StringBuilder str = new StringBuilder();
                    type.format(keySource, AkibanAppender.of(str));
                    keyValue = str.toString();
                }
                if (willUseBinaryTag(keyValue)) {
                    // Otherwise it would be ambiguous when reading.
                    keyValue = getRawSegment(key, i);
                }
            }
            result.add(keyValue);
        }
        return result;
    }

    /** If the type's internal representation corresponds to a Java
     * type for which there is standard YAML tag, can use
     * it. Otherwise, must resort to string, either because the
     * internal value isn't friendly (<code>Date</code> is a <code>Long</code>) 
     * or isn't standard (<code>Decimal</code> turns into <code>!!float</code>).
     */
    protected static boolean convertToType(TInstance type) {
        TClass tclass = TInstance.tClass(type);
        return ((tclass.getClass() == MNumeric.class) ||
                (tclass == AkBool.INSTANCE));
    }

    /** If a collated key isn't recoverable, we output the raw collating bytes. 
     * Return the given segment, <em>including</em> the terminating NUL byte.
     */
    protected static byte[] getRawSegment(Key key, int depth) {
        key.indexTo(depth);
        byte[] encoded = key.getEncodedBytes();
        int start = key.getIndex();
        int end = start;
        while (encoded[end++] != 0);
        byte[] result = new byte[end - start];
        System.arraycopy(encoded, start, result, 0, result.length);
        return result;
    }

    /** If an ordinary key segment value would write to YAML as a
     * binary array, couldn't tell it from a raw segment. So, need to
     * encode it raw, too.
     */
    protected static boolean willUseBinaryTag(Object value) {
        if (value instanceof byte[])
            return true;
        if (value instanceof String)
            return org.yaml.snakeyaml.reader.StreamReader.NON_PRINTABLE
                .matcher((String)value).find();
        return false;
    }

}
