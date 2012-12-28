/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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

    private final int firstColumn;
    private final int columnCount;
    private final List<HistogramEntry> entries;
}
