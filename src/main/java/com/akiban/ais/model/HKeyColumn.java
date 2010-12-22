package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKeyColumn
{
    public HKeySegment segment()
    {
        return segment;
    }

    public Column column()
    {
        return column;
    }

    public UserTable pkLessTable()
    {
        return pkLessTable;
    }

    public List<Column> equivalentColumns()
    {
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
        if (column.getTable().isGroupTable()) {
            GroupTable groupTable = (GroupTable) column.getTable();
            this.equivalentColumns = groupTable.matchingColumns(column);
        } else {
            this.equivalentColumns = new ArrayList<Column>();
            this.equivalentColumns.add(column);
        }
    }

    public HKeyColumn(HKeySegment segment, UserTable pkLessTable)
    {
        this.segment = segment;
        this.pkLessTable = pkLessTable;
        this.positionInHKey = segment.positionInHKey() + segment.columns().size() + 1;
        this.equivalentColumns = new ArrayList<Column>();
    }

    // State

    private HKeySegment segment;
    // A PK-less table can appear as a leaf table in a group. (It can't be non-leaf because a table without a PK
    // can't have an akiban FK referencing it.) A PK-less table has no PK columns to contribute to the hkey, so
    // a table counter is used instead. In this case, column = null and pkLessTable identifies the table. Otherwise,
    // column references the column showing up in the hkey and pkLessTable is null.
    private Column column;
    private UserTable pkLessTable;
    private int positionInHKey;
    // If column is a group table column, then we need to know all columns in the group table that are constrained
    // to have matching values, e.g. customer$cid and order$cid. For a user table, equivalentColumns contains just
    // column.
    private List<Column> equivalentColumns;
}
