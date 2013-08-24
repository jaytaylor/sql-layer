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

package com.foundationdb.server.store.statistics;

import com.foundationdb.ais.model.Index;

import java.util.List;

public class Histogram
{
    @Override
    public String toString()
    {
        return toString(null);
    }

    public String toString(Index index)
    {
        StringBuilder str = new StringBuilder(getClass().getSimpleName());
        if (index != null) {
            str.append(" for ").append(index.getIndexName()).append("(");
            for (int j = 0; j < columnCount; j++) {
                if (j > 0) str.append(", ");
                str.append(index.getKeyColumns().get(firstColumn + j).getColumn().getName());
            }
            str.append("):\n");
        }
        str.append(entries);
        return str.toString();
    }

    public int getFirstColumn()
    {
        return firstColumn;
    }

    public int getColumnCount()
    {
        return columnCount;
    }

    public List<HistogramEntry> getEntries() {
        return entries;
    }

    public IndexStatistics getIndexStatistics() {
        return indexStatistics;
    }

    public long totalDistinctCount()
    {
        long total = 0;
        for (HistogramEntry entry : entries) {
            if (entry.getEqualCount() > 0)
                total++;
            total += entry.getDistinctCount();
        }
        return total;
    }

    public Histogram(int firstColumn, int columnCount, List<HistogramEntry> entries)
    {
        this.firstColumn = firstColumn;
        this.columnCount = columnCount;
        this.entries = entries;
    }

    void setIndexStatistics(IndexStatistics indexStatistics) {
        this.indexStatistics = indexStatistics;
    }

    private IndexStatistics indexStatistics;
    private final int firstColumn;
    private final int columnCount;
    private final List<HistogramEntry> entries;
}
