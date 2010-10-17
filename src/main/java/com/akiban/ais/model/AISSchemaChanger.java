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
        updateTableName( index.getTable() );
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
