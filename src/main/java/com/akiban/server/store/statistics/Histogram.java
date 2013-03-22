
package com.akiban.server.store.statistics;

import com.akiban.ais.model.Index;

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
