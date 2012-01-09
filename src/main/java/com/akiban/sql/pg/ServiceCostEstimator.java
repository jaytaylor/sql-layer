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

package com.akiban.sql.pg;

import com.akiban.sql.optimizer.rule.CostEstimator;

import com.akiban.ais.model.Index;
import com.akiban.server.service.session.Session;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsService;

// TODO: Maybe move this someplace else. Right now this is where things meet.
public class ServiceCostEstimator extends CostEstimator
{
    private Session session;
    private IndexStatisticsService indexStatistics;

    public ServiceCostEstimator(PostgresServiceRequirements reqs,
                                Session session) {
        this.session = session;
        indexStatistics = reqs.indexStatistics();
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return indexStatistics.getIndexStatistics(session, index);
    }
}
