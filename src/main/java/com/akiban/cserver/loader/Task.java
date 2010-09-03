package com.akiban.cserver.loader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.TableName;
import com.akiban.ais.model.UserTable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
        return hKey;
    }

    public final List<Column> order()
    {
        return order;
    }

    public final String sql()
    {
        return sql;
    }

    // parentColumns are columns in join.parent. Return the corresponding
    // columns in join.child. If a column is
    // not present in join.parent, it is dropped.
    public static List<Column> columnsInChild(List<Column> parentColumns, Join join)
    {
        List<Column> childColumns = new ArrayList<Column>();
        for (Column parentColumn : parentColumns) {
            Column childColumn = join.getMatchingChild(parentColumn);
            if (childColumn != null) {
                childColumns.add(childColumn);
            }
        }
        return childColumns;
    }

    protected void addColumns(List<Column> columns)
    {
        this.columns.addAll(columns);
    }

    protected void addColumn(Column column)
    {
        this.columns.add(column);
    }

    protected void hKey(List<Column> hKey)
    {
        if (this.hKey == null) {
            this.hKey = hKey;
        } else {
            throw new BulkLoader.InternalError(hKey.toString());
        }
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

    // Returns table columns that participate in the hkey. Result might not be a
    // complete hkey.
    protected static List<Column> hKeyColumns(UserTable table)
    {
        return table.getParentJoin() == null
               ? table.getPrimaryKey().getColumns()
               : hKeyColumns(table.getParentJoin());
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
        return quote(tableName.getSchemaName()) + '.'
               + quote(tableName.getTableName());
    }

    protected TableName sourceTableName(TableName targetTableName)
    {
        return new TableName(loader.sourceSchema(targetTableName.getSchemaName()), targetTableName.getTableName());
    }

    protected static String quote(String s)
    {
        return '`' + s + '`';
    }

    private static List<Column> hKeyColumns(Join join)
    {
        UserTable joinParent = join.getParent();
        List<Column> parentHKeyColumns = joinParent.getParentJoin() == null ? joinParent
                .getPrimaryKey().getColumns()
                                                                            : hKeyColumns(joinParent.getParentJoin());
        // if hkey column in parent has no counterpart in child, it is dropped
        // by columnsInChild.
        List<Column> hKeyColumns = columnsInChild(parentHKeyColumns, join);
        // Add primary key columns from child not already in childHKeyColumns
        List<Column> childKey = join.getChild().getPrimaryKey().getColumns();
        for (Column childKeyColumn : childKey) {
            if (!hKeyColumns.contains(childKeyColumn)) {
                hKeyColumns.add(childKeyColumn);
            }
        }
        return hKeyColumns;
    }

    private List<Column> hKey;
    private final BulkLoader loader;
    private final UserTable table;
    private TableName artifactTableName;
    private String sql;
    private final List<Column> columns = new ArrayList<Column>();
    private List<Column> order;
}
