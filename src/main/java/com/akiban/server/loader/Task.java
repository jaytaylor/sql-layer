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
