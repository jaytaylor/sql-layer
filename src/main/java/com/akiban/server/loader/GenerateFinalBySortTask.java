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

package com.akiban.server.loader;

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
        order(hKey());
        sql(String.format(SQL_TEMPLATE,
                          quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(columns()),
                          commaSeparatedColumnNames(columns()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(hKey())));
        columnPositions = new int[columns().size()];
        int p = 0;
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
                              artifactTableName(), type(), toString(hKeyColumnPositions()));
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) engine = myisam select %s from %s order by %s";
}
