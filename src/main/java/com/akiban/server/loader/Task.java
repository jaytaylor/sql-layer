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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;

public abstract class Task
{
    public abstract String type();

    public final String artifactsSchema()
    {
        return loader.artifactsSchema();
    }

    public final UserTable table()
    {
        return table;
    }

    public final List<Column> columns()
    {
        return Collections.unmodifiableList(columns);
    }

    public final TableName artifactTableName()
    {
        return artifactTableName;
    }

    public final List<Column> hKey()
    {
        return table.allHKeyColumns();
    }

    public final List<Column> order()
    {
        return order;
    }

    public final String sql()
    {
        return sql;
    }

    protected void addColumns(List<Column> columns)
    {
        this.columns.addAll(columns);
    }

    protected void addColumn(Column column)
    {
        this.columns.add(column);
    }

    protected void order(List<Column> order)
    {
        if (this.order == null) {
            this.order = order;
        } else {
            throw new BulkLoader.InternalError(order.toString());
        }
    }

    protected void sql(String sql)
    {
        this.sql = sql;
    }

    protected Task(BulkLoader loader, UserTable table, String artifactSuffix)
    {
        this.loader = loader;
        this.table = table;
        artifactTableName = new TableName(artifactsSchema(), String.format(
            "%s%s", table.getName().getTableName(), artifactSuffix));
    }

    protected static String commaSeparatedColumnNames(List<Column> columns)
    {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (Column column : columns) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(quote(column.getName()));
        }
        return buffer.toString();
    }

    protected static String commaSeparatedColumnDeclarations(
        List<Column> columns)
    {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (Column column : columns) {
            if (first) {
                first = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(quote(column.getName()));
            buffer.append(' ');
            buffer.append(column.getType().name());
            if (column.getType().nTypeParameters() > 0) {
                buffer.append('(');
                buffer.append(column.getTypeParameter1());
                if (column.getType().nTypeParameters() > 1) {
                    buffer.append(", ");
                    buffer.append(column.getTypeParameter2());
                }
                buffer.append(')');
            }
        }
        return buffer.toString();
    }

    protected static String quote(TableName tableName)
    {
        return quote(tableName.getSchemaName()) + '.' + quote(tableName.getTableName());
    }

    protected TableName sourceTableName(TableName targetTableName)
    {
        return new TableName(loader.sourceSchema(targetTableName.getSchemaName()), targetTableName.getTableName());
    }

    protected static String quote(String s)
    {
        return '`' + s + '`';
    }

    private final BulkLoader loader;
    private final UserTable table;
    private TableName artifactTableName;
    private String sql;
    private final List<Column> columns = new ArrayList<Column>();
    private List<Column> order;
}
