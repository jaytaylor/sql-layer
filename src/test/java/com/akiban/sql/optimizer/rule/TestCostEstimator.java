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
import com.akiban.sql.optimizer.OptimizerTestBase;

import com.akiban.qp.rowtype.Schema;

import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsYamlLoader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.io.File;
import java.io.IOException;

public class TestCostEstimator extends CostEstimator
{
    private final AkibanInformationSchema ais;
    private final Map<Index,IndexStatistics> stats;

    public TestCostEstimator(AkibanInformationSchema ais, Schema schema, 
                             File statsFile, boolean statsIgnoreMissingIndexes) 
            throws IOException {
        super(schema);
        this.ais = ais;
        if (statsFile == null)
            stats = Collections.<Index,IndexStatistics>emptyMap();
        else
            stats = new IndexStatisticsYamlLoader(ais, OptimizerTestBase.DEFAULT_SCHEMA)
                .load(statsFile, statsIgnoreMissingIndexes);
    }

    public TestCostEstimator(AkibanInformationSchema ais, Schema schema, File statsFile)
            throws IOException {
        this(ais, schema, statsFile, false);
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return stats.get(index);
    }

}
