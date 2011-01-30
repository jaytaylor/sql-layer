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

import java.util.ArrayList;
import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

public class GenerateChildTask extends Task
{
    @Override
    public String type()
    {
        return "GenerateChild";
    }

    public GenerateChildTask(BulkLoader loader, UserTable table)
    {
        super(loader, table, "$child");
        // Get the child columns of the join connecting child table to parent,
        // in the same order as the parent's
        // primary key columns.
        this.table = table;
        Join parentJoin = table.getParentJoin();
        for (Column parentPKColumn : parentJoin.getParent().getPrimaryKey().getColumns()) {
            Column childFKColumn = parentJoin.getMatchingChild(parentPKColumn);
            if (childFKColumn == null) {
                throw new BulkLoader.InternalError(parentPKColumn.toString());
            }
            this.fkColumns.add(childFKColumn);
        }
        // The columns of the $child table are joinColumns and PK columns not
        // already included in joinColumns.
        addColumns(this.fkColumns);
        // Order by fk columns
        order(this.fkColumns);
        for (Column pkColumn : table.getPrimaryKey().getColumns()) {
            if (!this.fkColumns.contains(pkColumn)) {
                addColumn(pkColumn);
            }
        }
        sql(String.format(SQL_TEMPLATE, quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(columns()),
                          commaSeparatedColumnNames(columns()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(this.fkColumns)));
        loader.tracker().info("%s %s columns: %s", artifactTableName(), type(), columns());
        loader.tracker().info("%s %s hkey: %s", artifactTableName(), type(), hKey());
        loader.tracker().info("%s %s fkColumns: %s", artifactTableName(), type(), this.fkColumns);
        loader.tracker().info("%s %s order: %s", artifactTableName(), type(), order());
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) engine = myisam select %s from %s order by %s";

    protected final UserTable table;
    protected final List<Column> fkColumns = new ArrayList<Column>();
}