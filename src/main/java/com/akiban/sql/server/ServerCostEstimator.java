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
