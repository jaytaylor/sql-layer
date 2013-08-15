/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.server.service.session.Session;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.IndexVisitor;
import com.foundationdb.server.store.Store;

import java.util.ArrayList;
import java.util.List;

public class IndexStatisticsVisitor<K extends Comparable<? super K>, V> extends IndexVisitor<K,V>
{
    public interface VisitorCreator<K extends Comparable<? super K>, V> {
        IndexStatisticsGenerator<K,V> multiColumnVisitor(Index index);
        IndexStatisticsGenerator<K,V> singleColumnVisitor(Session session, IndexColumn indexColumn);
    }

    public IndexStatisticsVisitor(Session session,
                                  Index index,
                                  long indexRowCount,
                                  VisitorCreator<K,V> creator)
    {
        this.index = index;
        this.indexRowCount = indexRowCount;
        this.multiColumnVisitor = creator.multiColumnVisitor(index);
        this.singleColumnVisitors = new ArrayList<>();
        this.nIndexColumns = index.getKeyColumns().size();
        for (int f = 0; f < nIndexColumns; f++) {
            singleColumnVisitors.add(
                    creator.singleColumnVisitor(session, index.getKeyColumns().get(f))
            );
        }
    }

    public void init(int bucketCount)
    {
        multiColumnVisitor.init(bucketCount, indexRowCount);
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).init(bucketCount, -1L); // Row count computed by the single-column visitor
        }
    }

    public void finish(int bucketCount)
    {
        multiColumnVisitor.finish(bucketCount);
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).finish(bucketCount);
        }
    }

    protected void visit(K key, V value)
    {
        multiColumnVisitor.visit(key, value);
        for (int c = 0; c < nIndexColumns; c++) {
            singleColumnVisitors.get(c).visit(key, value);
        }
    }

    public IndexStatistics getIndexStatistics()
    {
        IndexStatistics indexStatistics = new IndexStatistics(index);
        // The multi-column visitor has the row count. The single-column visitors have the count of distinct
        // keys for that column.
        int rowCount = multiColumnVisitor.rowCount();
        indexStatistics.setRowCount(rowCount);
        indexStatistics.setSampledCount(rowCount);
        multiColumnVisitor.getIndexStatistics(indexStatistics);
        for (IndexStatisticsGenerator<K,V> singleColumnVisitor : singleColumnVisitors) {
            singleColumnVisitor.getIndexStatistics(indexStatistics);
        }
        return indexStatistics;
    }

    private final Index index;
    private final long indexRowCount;
    private final IndexStatisticsGenerator<K,V> multiColumnVisitor;
    private final List<IndexStatisticsGenerator<K,V>> singleColumnVisitors;
    private final int nIndexColumns;
}
