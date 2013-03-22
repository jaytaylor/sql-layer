
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
        this.tableNameMap = new HashMap<>();
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
    public void visitColumn(Column column)
    {
        updateTableName( column.getTable() );
    }

    @Override
    public void visitGroup(Group group) 
    {
    }

    @Override
    public void visitIndex(Index index)
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
    public void visitIndexColumn(IndexColumn indexColumn)
    {
        visitColumn( indexColumn.getColumn() );
        visitIndex( indexColumn.getIndex() );
    }

    @Override
    public void visitJoin(Join join)
    {
    }

    @Override
    public void visitJoinColumn(JoinColumn joinColumn)
    {
    }

    @Override
    public void visitType(Type type)
    {
    }

    @Override
    public void visitUserTable(UserTable userTable)
    {
        updateTableName( userTable );
    }
}
