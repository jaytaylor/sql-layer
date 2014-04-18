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

package com.foundationdb.sql.server;

import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.Table;
import com.foundationdb.server.service.tree.KeyCreator;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.store.statistics.IndexStatisticsService;

public class ServerCostEstimator extends CostEstimator
{
    private ServerSession session;
    private IndexStatisticsService indexStatistics;
    private boolean scaleIndexStatistics;
    private boolean testMode;

    public ServerCostEstimator(ServerSession session,
                               ServerServiceRequirements reqs,
                               ServerOperatorCompiler compiler, KeyCreator keyCreator) {
        super(compiler, keyCreator, reqs.costModel());
        this.session = session;
        indexStatistics = reqs.indexStatistics();
        scaleIndexStatistics = Boolean.parseBoolean(getProperty("scaleIndexStatistics", "true"));
        testMode = reqs.config().testing();
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return indexStatistics.getIndexStatistics(session.getSession(), index);
    }

    @Override
    public long getTableRowCount(Table table) {
        if (!scaleIndexStatistics) {
            // Unscaled test mode: return count from statistics, if present.
            long count = getTableRowCountFromStatistics(table);
            if (count >= 0)
                return count;
        }
        return table.rowDef().getTableStatus().getApproximateRowCount(session.getSession());
    }

    @Override
    protected void missingStats(Index index, Column column) {
        if (!testMode) {
            indexStatistics.missingStats(session.getSession(), index, column);
        }
    }

    @Override
    protected void checkRowCountChanged(Table table, IndexStatistics stats, long rowCount) {
        if (!testMode) {
            indexStatistics.checkRowCountChanged(session.getSession(), table, stats, rowCount);
        }
    }

}
