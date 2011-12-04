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

import com.akiban.server.PersistitKeyValueTarget;
import com.akiban.server.types.conversion.Converters;
import com.persistit.Key;

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

    private PersistitKeyValueTarget keyTarget = new PersistitKeyValueTarget();
    
    public IndexStatisticsYamlLoader(AkibanInformationSchema ais, String defaultSchema) {
        this.ais = ais;
        this.defaultSchema = defaultSchema;
    }
    
    public Map<Index,IndexStatistics> load(File file) throws IOException {
        Map<Index,IndexStatistics> result = new HashMap<Index,IndexStatistics>();
        Yaml yaml = new Yaml();
        FileInputStream istr = new FileInputStream(file);
        for (Object doc : yaml.loadAll(istr)) {
            IndexStatistics indexStatistics = parseStatistics(doc);
            result.put(indexStatistics.getIndex(), indexStatistics);
        }
        istr.close();
        return result;
    }

    protected IndexStatistics parseStatistics(Object obj) {
        if (!(obj instanceof Map))
            throw new AkibanInternalException("Document not in expected format");
        Map map = (Map)obj;
        TableName tableName = TableName.create(defaultSchema, (String)map.get("Table"));
        Table table = ais.getTable(tableName);
        if (table == null)
            throw new NoSuchTableException(tableName);
        String indexName = (String)map.get("Index");
        Index index = table.getIndex(indexName);
        if (index == null) {
            index = table.getGroup().getIndex(indexName);
            if (index == null)
                throw new NoSuchIndexException(indexName);
        }
        IndexStatistics result = new IndexStatistics(index);
        for (Object e : (Iterable)map.get("Statistics")) {
            Map em = (Map)e;
            int columnCount = (Integer)em.get("Columns");
            Histogram h = parseHistogram(em.get("Histogram"), index, columnCount);
            result.setHistogram(columnCount, h);
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
            Map emap = (Map)eobj;
            String keyString = emap.get("key").toString();
            byte[] keyBytes = null;
            int eqCount = (Integer)emap.get("eq");
            int ltCount = (Integer)emap.get("lt");
            int distinctCount = (Integer)emap.get("distinct");
            entries.add(new HistogramEntry(keyString, keyBytes,
                                           eqCount, ltCount, distinctCount));
        }
        return new Histogram(index, columnCount, entries);
    }

}
