
package com.akiban.sql.server;

import com.akiban.sql.optimizer.rule.cost.CostEstimator;

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.server.service.tree.KeyCreator;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;

public class ServerCostEstimator extends CostEstimator
{
    private ServerSession session;
    private IndexStatisticsService indexStatistics;
    private boolean scaleIndexStatistics;

    public ServerCostEstimator(ServerSession session,
                               ServerServiceRequirements reqs,
                               ServerOperatorCompiler compiler, KeyCreator keyCreator) {
        super(compiler, keyCreator);
        this.session = session;
        indexStatistics = reqs.indexStatistics();
        scaleIndexStatistics = Boolean.parseBoolean(getProperty("scaleIndexStatistics", "true"));
        if (reqs.config().testing())
            warningsEnabled = false;
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
        return table.rowDef().getTableStatus().getApproximateRowCount();
    }

}
