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
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.TableName;

import com.akiban.server.PersistitKeyPValueSource;
import com.akiban.server.PersistitKeyPValueTarget;
import com.akiban.server.PersistitKeyValueSource;
import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.collation.AkCollator;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.Types3Switch;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValue;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.util.AkibanAppender;
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
        this.defaultSchema = defaultSchema;
        key = keyCreator.createKey();
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
        int firstSpatialColumn = Integer.MAX_VALUE;
        if (index.isSpatial()) {
            firstSpatialColumn = index.firstSpatialArgument();
        }
        key.clear();
        if (Types3Switch.ON) {
            PersistitKeyPValueTarget keyTarget = new PersistitKeyPValueTarget();
            keyTarget.attach(key);
            for (int i = 0; i < columnCount; i++) {
                Object value = values.get(i);
                if (value instanceof byte[]) {
                    appendRawSegment((byte[])value);
                    continue;
                }
                TInstance tInstance;
                AkType akType; 
                AkCollator collator;
                if (i == firstSpatialColumn) {
                    tInstance = MNumeric.BIGINT.instance(true);
                    akType = AkType.LONG;
                    collator = null;
                }
                else {
                    int offset = i;
                    if (i > firstSpatialColumn) {
                        offset += index.dimensions() - 1;
                    }
                    Column column = index.getKeyColumns().get(offset).getColumn();
                    tInstance = column.tInstance();
                    akType = column.getType().akType();
                    collator = column.getCollator();
                }
                // For example, for DECIMAL, value will be a
                // String, pvalue will be a its VARCHAR, and pvalue2
                // will be a BigDecimalWrapper, which only
                // MBigDecimal.writeCollating knows how to unwrap into
                // a Key.
                TPreptimeValue pvalue = PValueSources.fromObject(value, akType);
                TExecutionContext context = new TExecutionContext(null,
                                                                  Collections.singletonList(pvalue.instance()),
                                                                  tInstance,
                                                                  null, null, null, null);
                PValue pvalue2 = new PValue(tInstance.typeClass().underlyingType());
                tInstance.typeClass().fromObject(context, pvalue.value(), pvalue2);
                tInstance.writeCollating(pvalue2, keyTarget);
            }
        }
        else {
            FromObjectValueSource valueSource = new FromObjectValueSource();
            PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget();
            keyTarget.attach(key);
            for (int i = 0; i < columnCount; i++) {
                Object value = values.get(i);
                if (value instanceof byte[]) {
                    appendRawSegment((byte[])value);
                    continue;
                }
                AkType akType; 
                AkCollator collator;
                if (i == firstSpatialColumn) {
                    akType = AkType.LONG;
                    collator = null;
                }
                else {
                    int offset = i;
                    if (i > firstSpatialColumn) {
                        offset += index.dimensions() - 1;
                    }
                    Column column = index.getKeyColumns().get(offset).getColumn();
                    akType = column.getType().akType();
                    collator = column.getCollator();
                }
                valueSource.setReflectively(value);
                keyTarget.expectingType(akType, collator);
                Converters.convert(valueSource, keyTarget);
            }
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
        if (index.isSpatial()) nkeys -= index.dimensions() - 1;
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
        int firstSpatialColumn = Integer.MAX_VALUE;
        if (index.isSpatial()) {
            firstSpatialColumn = index.firstSpatialArgument();
        }
        List<Object> result = new ArrayList<Object>(columnCount);
        if (Types3Switch.ON) {
            for (int i = 0; i < columnCount; i++) {
                TInstance tInstance;
                AkType akType; 
                boolean useRawSegment;
                if (i == firstSpatialColumn) {
                    tInstance = MNumeric.BIGINT.instance(true);
                    akType = AkType.LONG;
                    useRawSegment = false;
                }
                else {
                    int offset = i;
                    if (i > firstSpatialColumn) {
                        offset += index.dimensions() - 1;
                    }
                    Column column = index.getKeyColumns().get(offset).getColumn();
                    tInstance = column.tInstance();
                    akType = column.getType().akType();
                    AkCollator collator = column.getCollator();
                    useRawSegment = ((collator != null) && !collator.isRecoverable());
                }
                Object keyValue;
                if (useRawSegment) {
                    keyValue = getRawSegment(key, i);
                }
                else {
                    PersistitKeyPValueSource keySource = new PersistitKeyPValueSource(tInstance);
                    keySource.attach(key, i, tInstance);
                    if (convertToType(akType)) {
                        keyValue = PValueSources.toObject(keySource, akType);
                    }
                    else if (keySource.isNull()) {
                        keyValue = null;
                    }
                    else {
                        StringBuilder str = new StringBuilder();
                        tInstance.format(keySource, AkibanAppender.of(str));
                        keyValue = str.toString();
                    }
                    if (willUseBinaryTag(keyValue)) {
                        // Otherwise it would be ambiguous when reading.
                        keyValue = getRawSegment(key, i);
                    }
                }
                result.add(keyValue);
            }
        }
        else {
            PersistitKeyValueSource keySource = new PersistitKeyValueSource();
            ToObjectValueTarget valueTarget = new ToObjectValueTarget();
            for (int i = 0; i < columnCount; i++) {
                AkType akType; 
                AkCollator collator;
                boolean useRawSegment;
                if (i == firstSpatialColumn) {
                    akType = AkType.LONG;
                    collator = null;
                    useRawSegment = false;
                }
                else {
                    int offset = i;
                    if (i > firstSpatialColumn) {
                        offset += index.dimensions() - 1;
                    }
                    Column column = index.getKeyColumns().get(offset).getColumn();
                    akType = column.getType().akType();
                    collator = column.getCollator();
                    useRawSegment = ((collator != null) && !collator.isRecoverable());
                }
                Object keyValue;
                if (useRawSegment) {
                    keyValue = getRawSegment(key, i);
                }
                else {
                    keySource.attach(key, i, akType, collator);
                    valueTarget.expectType(convertToType(akType) ? akType : AkType.VARCHAR);
                    Converters.convert(keySource, valueTarget);
                    keyValue = valueTarget.lastConvertedValue();
                    if (willUseBinaryTag(keyValue)) {
                        // Otherwise it would be ambiguous when reading.
                        keyValue = getRawSegment(key, i);
                    }
                }
                result.add(keyValue);
            }
        }
        return result;
    }

    /** If the AkType's internal representation corresponds to a Java
     * type for which there is standard YAML tag, can use
     * it. Otherwise, must resort to string, either because the
     * internal value isn't friendly (<code>Date</code> is a <code>Long</code>) 
     * or isn't standard (<code>Decimal</code> turns into <code>!!float</code>).
     */
    protected static boolean convertToType(AkType sourceType) {
        switch (sourceType) {
        case DOUBLE:
        case FLOAT:
        case INT:
        case LONG:
        case BOOL:
            return true;
        default:
            return false;
        }
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
