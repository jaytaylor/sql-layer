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
            buffer.append(segment.toString());
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

    public int getMaxStorageSize() {
        int total = 0;
        for(HKeySegment segment : segments) {
            total += segment.getMaxStorageSize();
        }
        return total;
    }

    public HKey()
    {}

    // State

    private Table table;
    private List<HKeySegment> segments = new ArrayList<HKeySegment>();
}
