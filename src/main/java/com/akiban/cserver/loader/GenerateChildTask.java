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
        Join parentJoin = table.getParentJoin();
        for (Column parentPKColumn : parentJoin.getParent().getPrimaryKey()
                .getColumns()) {
            Column childFKColumn = parentJoin.getMatchingChild(parentPKColumn);
            if (childFKColumn == null) {
                throw new BulkLoader.InternalError(parentPKColumn.toString());
            }
            fkColumns.add(childFKColumn);
        }
        // The columns of the $child table are joinColumns and PK columns not
        // already included in joinColumns.
        addColumns(fkColumns);
        // Order by fk columns
        order(fkColumns);
        for (Column pkColumn : table.getPrimaryKey().getColumns()) {
            if (!fkColumns.contains(pkColumn)) {
                addColumn(pkColumn);
            }
        }
        sql(String.format(SQL_TEMPLATE, quote(artifactTableName()),
                          commaSeparatedColumnDeclarations(columns()),
                          commaSeparatedColumnNames(columns()),
                          quote(sourceTableName(table.getName())),
                          commaSeparatedColumnNames(fkColumns)));
    }

    private static final String SQL_TEMPLATE = "create table %s(%s) select %s from %s order by %s";

    protected final List<Column> fkColumns = new ArrayList<Column>();
}