package com.akiban.ais.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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
        this.index = index;
        this.column = column;
        this.position = position;
        this.ascending = ascending;
        this.indexedLength = indexedLength;
    }
    
    public static IndexColumn create(AkibaInformationSchema ais, Map<String, Object> map)
    {
        IndexColumn indexColumn = null;
        String schemaName = (String) map.get(indexColumn_schemaName);
        String tableName = (String) map.get(indexColumn_tableName);
        String indexName = (String) map.get(indexColumn_indexName);
        String columnName = (String) map.get(indexColumn_columnName);
        Integer position = (Integer) map.get(indexColumn_position);
        Boolean ascending = (Boolean) map.get(indexColumn_ascending);
        Integer indexedLength = (Integer) map.get(indexColumn_indexedLength);
        Table table = ais.getTable(schemaName, tableName);
        if (table != null) {
            Index index = table.getIndex(indexName);
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
        map.put(indexColumn_schemaName, index.getIndexName().getSchemaName());
        map.put(indexColumn_tableName, index.getIndexName().getTableName());
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
