/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
