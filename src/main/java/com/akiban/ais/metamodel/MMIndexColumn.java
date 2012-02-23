/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.ais.metamodel;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.HashMap;
import java.util.Map;

public class MMIndexColumn implements ModelNames {
    public static IndexColumn create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        IndexColumn indexColumn = null;
        String schemaName = (String) map.get(indexColumn_schemaName);
        String tableName = (String) map.get(indexColumn_tableName);
        String indexType = (String) map.get(indexColumn_indexType);
        String indexName = (String) map.get(indexColumn_indexName);
        String columnName = (String) map.get(indexColumn_columnName);
        Integer position = (Integer) map.get(indexColumn_position);
        Boolean ascending = (Boolean) map.get(indexColumn_ascending);
        Integer indexedLength = (Integer) map.get(indexColumn_indexedLength);
        Table table = ais.getTable(schemaName, tableName);
        Index index = null;
        if(table != null) {
            if(Index.IndexType.GROUP.toString().endsWith(indexType)) {
                Group group = table.getGroup();
                if (group != null) {
                    index = group.getIndex(indexName);
                }
            }
            else {
                index = table.getIndex(indexName);
            }
            if (index != null) {
                Column column = table.getColumn(columnName.toLowerCase());
                if (column != null) {
                    indexColumn = IndexColumn.create(index, column, position, ascending, indexedLength);
                }
            }
        }
        return indexColumn;
    }

    public static Map<String, Object> map(IndexColumn indexColumn)
    {
        Map<String, Object> map = new HashMap<String, Object>();
        Column column = indexColumn.getColumn();
        map.put(indexColumn_schemaName, column.getTable().getName().getSchemaName());
        map.put(indexColumn_tableName, column.getTable().getName().getTableName());
        Index index = indexColumn.getIndex();
        map.put(indexColumn_indexType, index.getIndexType().toString());
        map.put(indexColumn_indexName, index.getIndexName().getName());
        map.put(indexColumn_columnName, column.getName());
        map.put(indexColumn_position, indexColumn.getPosition());
        map.put(indexColumn_ascending, indexColumn.isAscending());
        map.put(indexColumn_indexedLength, indexColumn.getIndexedLength());
        return map;
    }
}
 