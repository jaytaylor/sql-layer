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

package com.akiban.ais.model;

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
    
    public UserTable userTable()
    {
        return (UserTable) table;
    }

    public synchronized List<HKeySegment> segments()
    {
        return segments;
    }

    public synchronized int nColumns()
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

    public synchronized HKeySegment addSegment(UserTable segmentTable)
    {
        assert keyDepth == null : segmentTable; // Should only be computed after HKeySegments are completely formed.
        UserTable lastSegmentTable = segments.isEmpty() ? null : segments.get(segments.size() - 1).table();
        assert segmentTable.parentTable() == lastSegmentTable;
        HKeySegment segment = new HKeySegment(this, segmentTable);
        segments.add(segment);
        return segment;
    }

    public synchronized boolean containsColumn(Column column) {
        for(HKeySegment hkSeg : segments) {
            for(HKeyColumn hkCol : hkSeg.columns()) {
                if(hkCol.column() == column) {
                    return true;
                }
            }
        }
        return false;
    }

    public synchronized int[] keyDepth()
    {
        if (keyDepth == null) {
            computeKeyDepth();
        }
        return keyDepth;
    }

    // For use by this class

    private void computeKeyDepth()
    {
        keyDepth = new int[segments.size() + 1];
        int hKeySegments = segments.size();
        for (int hKeySegment = 0; hKeySegment <= hKeySegments; hKeySegment++) {
            this.keyDepth[hKeySegment] =
                hKeySegment == 0
                ? 0
                // + 1 to account for the ordinal
                : keyDepth[hKeySegment - 1] + 1 + segments.get(hKeySegment - 1).columns().size();
        }
    }

    // State

    private final Table table;
    private final List<HKeySegment> segments = new ArrayList<HKeySegment>();
    // keyDepth[n] is the number of key segments (ordinals + key values) comprising an hkey of n parts.
    // E.g. keyDepth[1] for the hkey of the root segment.
    private int[] keyDepth;
}
