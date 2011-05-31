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

import java.util.HashMap;

public class AISSchemaChanger implements Visitor
{
    private final String from;
    private final String to;
    private final HashMap<String,TableName> tableNameMap;
    
    public AISSchemaChanger(String from, String to)
    {
        this.from = from;
        this.to = to;
        this.tableNameMap = new HashMap<String,TableName>();
    }
    
    private void updateTableName(Table table)
    {
        TableName tableName = table.getName();
        if (!tableName.getSchemaName().equals(from)) {
            return;
        }
        TableName newName = tableNameMap.get(tableName.getTableName());
        if (newName == null) {
            newName = new TableName(to, tableName.getTableName());
            tableNameMap.put(tableName.getTableName(), newName);
        }
        table.setTableName(newName);
    }
    
    @Override
    public void visitColumn(Column column) throws Exception
    {
        updateTableName( column.getTable() );
    }

    @Override
    public void visitGroup(Group group) throws Exception
    {
    }

    @Override
    public void visitGroupTable(GroupTable groupTable) throws Exception
    {
    }

    @Override
    public void visitIndex(Index index) throws Exception
    {
        IndexName indexName = index.getIndexName();
        if (!indexName.getSchemaName().equals(from)) {
            return;
        }
        IndexName newName = new IndexName(new TableName(indexName.getSchemaName(), indexName.getTableName()),
                                          indexName.getName());
        index.setIndexName(newName);
    }

    @Override
    public void visitIndexColumn(IndexColumn indexColumn) throws Exception
    {
        visitColumn( indexColumn.getColumn() );
        visitIndex( indexColumn.getIndex() );
    }

    @Override
    public void visitJoin(Join join) throws Exception
    {
    }

    @Override
    public void visitJoinColumn(JoinColumn joinColumn) throws Exception
    {
    }

    @Override
    public void visitType(Type type) throws Exception
    {
    }

    @Override
    public void visitUserTable(UserTable userTable) throws Exception
    {
        updateTableName( userTable );
    }
}
