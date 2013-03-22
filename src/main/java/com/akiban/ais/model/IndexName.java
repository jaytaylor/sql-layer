
package com.akiban.ais.model;

public class IndexName implements Comparable<IndexName>
{
    private final TableName tableName;
    private final String indexName;

    public IndexName(TableName tableName, String indexName)
    {
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public String toString()
    {
        return tableName.toString() + "." + indexName;
    }

    public String getSchemaName()
    {
        return tableName.getSchemaName();
    }

    public String getTableName()
    {
        return tableName.getTableName();
    }

    public TableName getFullTableName() {
        return tableName;
    }

    public String getName()
    {
        return indexName;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (! (obj instanceof IndexName))
            return false;
        IndexName o = (IndexName) obj;

        return getSchemaName().equals(o.getSchemaName()) &&
               getTableName().equals(o.getTableName()) &&
               getName().equals(o.getName());
    }
    
    @Override
    public int hashCode()
    {
        return getSchemaName().hashCode() +
               getTableName().hashCode() +
               getName().hashCode();
    }

    @Override
    public int compareTo(IndexName o) {
        int c = tableName.compareTo(o.tableName);
        if(c == 0) {
            c = indexName.compareTo(o.indexName);
        }
        return c;
    }
}
