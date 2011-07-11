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

package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.validation.AISInvariants;

public class IndexColumn implements Serializable, ModelNames
{
    // IndexColumn interface

    @Override
    public String toString()
    {
        return "IndexColumn(" + column.getName() + ")";
    }

    public Index getIndex()
    {
        return index;
    }

    public Column getColumn()
    {
        return column;
    }

    public Integer getPosition()
    {
        return position;
    }

    public Integer getIndexedLength()
    {
        return indexedLength;
    }

    public Boolean isAscending()
    {
        return ascending;
    }

    @SuppressWarnings("unused")
    private IndexColumn()
    {
        // GWT requires empty constructor
    }
    
    public IndexColumn(Index index, Column column, Integer position, Boolean ascending, Integer indexedLength)
    {
        AISInvariants.checkDuplicateColumnsInIndex(index, column.getName());
        //AISInvariants.checkDuplicateIndexColumnPosition(index, position);
        
        this.index = index;
        this.column = column;
        this.position = position;
        this.ascending = ascending;
        this.indexedLength = indexedLength;
    }
    
    public static IndexColumn create(AkibanInformationSchema ais, Map<String, Object> map)
    {
        ais.checkMutability();
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
                Column column = table.getColumnMap().get(columnName.toLowerCase());
                if (column != null) {
                    indexColumn = new IndexColumn(index, column, position, ascending, indexedLength);
                    index.addColumn(indexColumn);
                }
            }
        }
        return indexColumn;
    }
    
    public Map<String, Object> map()
    {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put(indexColumn_schemaName, column.getTable().getName().getSchemaName());
        map.put(indexColumn_tableName, column.getTable().getName().getTableName());
        map.put(indexColumn_indexType, index.getIndexType().toString());
        map.put(indexColumn_indexName, index.getIndexName().getName());
        map.put(indexColumn_columnName, column.getName());
        map.put(indexColumn_position, position);
        map.put(indexColumn_ascending, ascending);
        map.put(indexColumn_indexedLength, indexedLength);
        return map;
    }

    // State

    private Index index;
    private Column column;
    private Integer position;
    private Boolean ascending;
    private Integer indexedLength;
}
