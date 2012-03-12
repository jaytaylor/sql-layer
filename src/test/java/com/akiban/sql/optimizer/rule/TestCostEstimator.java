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

package com.akiban.sql.optimizer.rule;

import com.akiban.ais.model.*;

import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsYamlLoader;

import java.util.Map;
import java.util.Collections;
import java.io.File;
import java.io.IOException;

public class TestCostEstimator extends CostEstimator
{
    private final AkibanInformationSchema ais;
    private final Map<Index,IndexStatistics> stats;

    public TestCostEstimator(AkibanInformationSchema ais, String defaultSchema,
                             File statsFile) 
            throws IOException {
        this.ais = ais;
        if (statsFile == null)
            stats = Collections.<Index,IndexStatistics>emptyMap();
        else
            stats = new IndexStatisticsYamlLoader(ais, defaultSchema).load(statsFile);
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return stats.get(index);
    }

    @Override
    public IndexStatistics[] getIndexColumnStatistics(Index index)
    {
        // Adapter from IndexStatisticsServiceImpl.getIndexCollumnStatistics
        IndexStatistics[] indexStatsArray = new IndexStatistics[index.getKeyColumns().size()];
        int i = 0;
        for (IndexColumn indexColumn : index.getKeyColumns()) {
            IndexStatistics indexStatistics = null;
            Column leadingColumn = indexColumn.getColumn();
            // Find a TableIndex whose first column is leadingColumn
            for (TableIndex tableIndex : leadingColumn.getTable().getIndexes()) {
                if (tableIndex.getKeyColumns().get(0).getColumn() == leadingColumn) {
                    indexStatistics = getIndexStatistics(tableIndex);
                    if (indexStatistics != null) {
                        break;
                    }
                }
            }
            // If none, find a GroupIndex whose first column is leadingColumn
            if (indexStatistics == null) {
                groupLoop: for (Group group : ais.getGroups().values()) {
                    for (GroupIndex groupIndex : group.getIndexes()) {
                        if (groupIndex.getKeyColumns().get(0).getColumn() == leadingColumn) {
                            indexStatistics = getIndexStatistics(groupIndex);
                            if (indexStatistics != null) {
                                break groupLoop;
                            }
                        }
                    }
                }
            }
            indexStatsArray[i++] = getIndexStatistics(index);
        }
        return indexStatsArray;
    }

    @Override
    public long getTableRowCount(Table table) {
        for (Index index : table.getIndexes()) {
            IndexStatistics istats = stats.get(index);
            if (istats != null)
                return istats.getRowCount();
        }
        return 1;
    }

    @Override
    protected boolean scaleIndexStatistics() {
        return false;
    }

}
