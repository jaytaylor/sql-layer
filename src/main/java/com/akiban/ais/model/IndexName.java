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

public class IndexName
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
