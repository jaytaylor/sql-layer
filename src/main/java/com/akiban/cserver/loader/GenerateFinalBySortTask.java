package com.akiban.cserver.loader;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public class GenerateFinalBySortTask extends GenerateFinalTask
{
    @Override
    public String type()
    {
        return "GenerateFinalBySort";
    }

    public GenerateFinalBySortTask(BulkLoader loader, UserTable table)
    {
        super(loader, table);
        addColumns(table.getColumns());
        hKey(hKeyColumns(table));
        order(hKey());
        sql(String.format(SQL_TEMPLATE,
                          quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(columns()),
                          commaSeparatedColumnNames(columns()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(hKey())));
        hKeyColumnPositions = new int[hKey().size()];
        int p = 0;
        for (Column column : hKey()) {
            int position = columns().indexOf(column);
            if (position == -1) {
                throw new BulkLoader.InternalError(column.toString());
            }
            hKeyColumnPositions[p++] = position;
        }
        columnPositions = new int[columns().size()];
        p = 0;
        for (Column column : columns()) {
            int position = columns().indexOf(column);
            if (position == -1) {
                throw new BulkLoader.InternalError(column.toString());
            }
            if (position != p) {
                throw new BulkLoader.InternalError(column.toString());
            }
            columnPositions[p++] = position;
        }
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
        loader.tracker().info("%s %s columnPositions: %s",
                              artifactTableName(), type(), toString(columnPositions));
        loader.tracker().info("%s %s hKeyColumnPositions: %s",
                              artifactTableName(), type(), toString(hKeyColumnPositions));
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) "
                                               + "select %s " + "from %s " + "order by %s";
}
