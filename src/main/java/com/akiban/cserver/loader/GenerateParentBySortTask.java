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
        addColumns(hKey());
        pkColumns(table.getPrimaryKey().getColumns());
        order(pkColumns());
        sql(String.format(SQL_TEMPLATE,
                          quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(hKey()),
                          commaSeparatedColumnNames(hKey()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(order())));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s pkColumns: %s", artifactTableName(), type(), pkColumns());
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) engine = myisam select %s from %s order by %s";
}