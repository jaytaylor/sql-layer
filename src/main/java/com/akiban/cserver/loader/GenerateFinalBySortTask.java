package com.akiban.cserver.loader;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

public class GenerateFinalBySortTask extends GenerateFinalTask {
    @Override
    public String type() {
        return "GenerateFinalBySort";
    }

    public GenerateFinalBySortTask(BulkLoader loader, UserTable table) {
        super(loader, table);
        hKey(hKeyColumns(table));
        order(hKey());
        sql(String.format(SQL_TEMPLATE, quote(artifactTableName()),
                commaSeparatedColumnDeclarations(table.getColumns()),
                commaSeparatedColumnNames(table.getColumns()),
                quote(sourceTableName(table.getName())),
                commaSeparatedColumnNames(hKey())));
        hKeyColumnPositions = new int[hKey().size()];
        int p = 0;
        for (Column column : hKey()) {
            int position = table.getColumns().indexOf(column);
            if (position == -1) {
                throw new BulkLoader.InternalError(column.toString());
            }
            hKeyColumnPositions[p++] = position;
        }
        columnPositions = new int[table.getColumns().size()];
        p = 0;
        for (Column column : table.getColumns()) {
            int position = table.getColumns().indexOf(column);
            if (position == -1) {
                throw new BulkLoader.InternalError(column.toString());
            }
            if (position != p) {
                throw new BulkLoader.InternalError(column.toString());
            }
            columnPositions[p++] = position;
        }
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) "
            + "select %s " + "from %s " + "order by %s";
}
