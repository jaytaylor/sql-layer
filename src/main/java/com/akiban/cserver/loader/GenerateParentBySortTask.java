package com.akiban.cserver.loader;

import com.akiban.ais.model.UserTable;

public class GenerateParentBySortTask extends GenerateParentTask
{
    @Override
    public String type()
    {
        return "GenerateParentBySort";
    }

    public GenerateParentBySortTask(BulkLoader loader, UserTable table)
    {
        super(loader, table);
        hKey(hKeyColumns(table));
        addColumns(hKey());
        pkColumns(table.getPrimaryKey().getColumns());
        order(pkColumns());
        sql(String.format(SQL_TEMPLATE, quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(hKey()),
                          commaSeparatedColumnNames(hKey()), quote(sourceTableName(table
                        .getName())), commaSeparatedColumnNames(order())));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s pkColumns: %s", artifactTableName(), type(), pkColumns());
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) "
                                               + "select %s " + "from %s " + "order by %s";
}