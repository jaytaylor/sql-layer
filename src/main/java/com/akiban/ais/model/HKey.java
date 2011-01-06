package com.akiban.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKey
{
    public String toString()
    {
        StringBuilder buffer = new StringBuilder();
        buffer.append("HKey(");
        boolean firstTable = true;
        for (HKeySegment segment : segments) {
            if (firstTable) {
                firstTable = false;
            } else {
                buffer.append(", ");
            }
            buffer.append(segment.table().getName().getTableName());
            buffer.append(": (");
            boolean firstColumn = true;
            for (HKeyColumn hKeyColumn : segment.columns()) {
                if (firstColumn) {
                    firstColumn = false;
                } else {
                    buffer.append(", ");
                }
                buffer.append(hKeyColumn.column().getTable().getName().getTableName());
                buffer.append('.');
                buffer.append(hKeyColumn.column().getName());
            }
            buffer.append(")");
        }
        buffer.append(")");
        return buffer.toString();
    }

    public List<HKeySegment> segments()
    {
        return segments;
    }

    public int nColumns()
    {
        int nColumns = 0;
        for (HKeySegment segment : segments) {
            nColumns += segment.columns().size();
        }
        return nColumns;
    }

    public HKey(Table table)
    {
        this.table = table;
    }

    public HKeySegment addSegment(UserTable table)
    {
        HKeySegment segment = new HKeySegment(this, table);
        UserTable parent = table.getParentJoin() == null ? null : table.getParentJoin().getParent();
        UserTable lastSegmentTable = segments.isEmpty() ? null : segments.get(segments.size() - 1).table();
        assert parent == lastSegmentTable;
        segments.add(segment);
        return segment;
    }

    public HKey()
    {}

    // State

    private Table table;
    private List<HKeySegment> segments = new ArrayList<HKeySegment>();
}
