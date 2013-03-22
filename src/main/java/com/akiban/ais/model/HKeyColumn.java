
package com.akiban.ais.model;

import java.util.Collections;
import java.util.List;

public class HKeyColumn
{
    @Override
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append(column().getTable().getName().getTableName());
        buffer.append('.');
        buffer.append(column().getName());
        return buffer.toString();
    }

    public HKeySegment segment()
    {
        return segment;
    }

    public Column column()
    {
        return column;
    }

    public List<Column> equivalentColumns()
    {
        if (equivalentColumns == null) {
            assert column.getTable().isGroupTable() : "null equivalentColumns on non-group-table column: " + column;
            throw new UnsupportedOperationException("group tables have no equivalent columns");
        }
        return equivalentColumns;
    }

    public int positionInHKey()
    {
        return positionInHKey;
    }

    public HKeyColumn(HKeySegment segment, Column column)
    {
        this.segment = segment;
        this.column = column;
        this.positionInHKey = segment.positionInHKey() + segment.columns().size() + 1;
        if (column.getTable().isUserTable()) {
            UserTable userTable = (UserTable) column.getTable();
            this.equivalentColumns = Collections.unmodifiableList(userTable.matchingColumns(column));
        } else {
            this.equivalentColumns = null;
        }
    }

    // For use by this class
    
    private void findDependentTables(Column column, UserTable table, List<UserTable> dependentTables)
    {
        boolean dependent = false;
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn hKeyColumn : segment.columns()) {
                dependent = dependent || hKeyColumn.column() == column;
            }
        }
        if (dependent) {
            dependentTables.add(table);
        }
        for (Join join : table.getChildJoins()) {
            findDependentTables(column, join.getChild(), dependentTables);
        }
    }

    // State

    private final HKeySegment segment;
    private final Column column;
    private final int positionInHKey;
    // If column is a group table column, then we need to know all columns in the group table that are constrained
    // to have matching values, e.g. customer$cid and order$cid. For a user table, equivalentColumns contains just
    // column.
    private final List<Column> equivalentColumns;
}
