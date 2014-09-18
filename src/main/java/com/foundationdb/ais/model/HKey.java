/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.ais.model;

import java.util.ArrayList;
import java.util.List;

public class HKey
{
    @Override
    public synchronized String toString()
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
            buffer.append(segment.toString());
        }
        buffer.append(")");
        return buffer.toString();
    }
    
    public Table table()
    {
        return table;
    }

    public List<HKeySegment> segments()
    {
        return segments;
    }

    public int nColumns()
    {
        ensureDerived();
        return columns.length;
    }
    
    public Column column(int i)
    {
        ensureDerived();
        return columns[i];
    }

    public HKey(Table table)
    {
        this.table = table;
    }

    public synchronized HKeySegment addSegment(Table segmentTable)
    {
        assert keyDepth == null : segmentTable; // Should only be computed after HKeySegments are completely formed.
        Table lastSegmentTable = segments.isEmpty() ? null : segments.get(segments.size() - 1).table();
        assert segmentTable.getParentTable() == lastSegmentTable;
        HKeySegment segment = new HKeySegment(this, segmentTable);
        segments.add(segment);
        return segment;
    }

    public boolean containsColumn(Column column) 
    {
        ensureDerived();
        for (Column c : columns) {
            if (c == column) {
                return true;
            }
        }
        return false;
    }

    public int[] keyDepth()
    {
        ensureDerived();
        return keyDepth;
    }

    // For use by this class

    private void ensureDerived()
    {
        if (columns == null) {
            synchronized (this) {
                if (columns == null) {
                    // columns
                    List<Column> columnList = new ArrayList<>();
                    for (HKeySegment segment : segments) {
                        for (HKeyColumn hKeyColumn : segment.columns()) {
                            columnList.add(hKeyColumn.column());
                        }
                    }
                    Column[] columnsTmp = new Column[columnList.size()];
                    int c = 0;
                    for (Column column : columnList) {
                        columnsTmp[c] = column;
                        c++;
                    }
                    // keyDepth
                    int[] keyDepthTmp = new int[segments.size() + 1];
                    int hKeySegments = segments.size();
                    for (int hKeySegment = 0; hKeySegment < hKeySegments; hKeySegment++) {
                        keyDepthTmp[hKeySegment] =
                            hKeySegment == 0
                            ? 1
                            // + 1 to account for the ordinal
                            : keyDepthTmp[hKeySegment - 1] + 1 + segments.get(hKeySegment - 1).columns().size();
                    }
                    keyDepthTmp[hKeySegments] = columnsTmp.length + hKeySegments;
                    columns = columnsTmp;
                    keyDepth = keyDepthTmp;
                }
            }
        }
    }

    // State

    private final Table table;
    private final List<HKeySegment> segments = new ArrayList<>();
    // keyDepth[n] is the number of key segments (ordinals + key values) comprising an hkey of n parts.
    // E.g. keyDepth[1] for the number of segments of the root hkey.
    //      keyDepth[2] for the number of key segments of the root's child + keyDepth[1].
    private volatile int[] keyDepth;
    private volatile Column[] columns;
}
