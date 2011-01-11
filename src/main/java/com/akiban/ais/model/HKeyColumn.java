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

    // State

    private HKeySegment segment;
    private Column column;
    private int positionInHKey;
    // If column is a group table column, then we need to know all columns in the group table that are constrained
    // to have matching values, e.g. customer$cid and order$cid. For a user table, equivalentColumns contains just
    // column.
    private List<Column> equivalentColumns;
}
