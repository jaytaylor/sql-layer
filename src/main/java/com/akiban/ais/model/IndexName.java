/* <GENERIC-HEADER - BEGIN>
 *
 * $(COMPANY) $(COPYRIGHT)
 *
 * Created on: Nov, 20, 2009
 * Created by: Thomas Hazel
 *
 * </GENERIC-HEADER - END> */

package com.akiban.ais.model;

import java.io.Serializable;

public class IndexName implements Serializable
{
    private Table table;
    private String indexName;

    @SuppressWarnings("unused")
    private IndexName()
    {
        // GWT
    }

    public IndexName(Table table, String indexName)
    {
        this.table = table;
        this.indexName = indexName;
    }

    @Override
    public String toString()
    {
        return indexName;
    }

    public String getSchemaName()
    {
        return table.getName().getSchemaName();
    }

    public String getTableName()
    {
        return table.getName().getTableName();
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
}
