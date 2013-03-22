
package com.akiban.sql.optimizer.rule.cost;

import com.akiban.ais.model.*;
import com.akiban.sql.optimizer.OptimizerTestBase;

import com.akiban.qp.rowtype.Schema;

import com.akiban.server.collation.TestKeyCreator;
import com.akiban.server.store.statistics.IndexStatistics;
import com.akiban.server.store.statistics.IndexStatisticsYamlLoader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestCostEstimator extends CostEstimator
{
    private final AkibanInformationSchema ais;
    private final Map<Index,IndexStatistics> stats;

    public TestCostEstimator(AkibanInformationSchema ais, Schema schema, 
                             File statsFile, boolean statsIgnoreMissingIndexes,
                             Properties properties)
            throws IOException {
        super(schema, properties, new TestKeyCreator());
        this.ais = ais;
        if (statsFile == null)
            stats = Collections.<Index,IndexStatistics>emptyMap();
        else
            stats = new IndexStatisticsYamlLoader(ais, OptimizerTestBase.DEFAULT_SCHEMA, new TestKeyCreator())
                .load(statsFile, statsIgnoreMissingIndexes);
        warningsEnabled = false;
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return stats.get(index);
    }

}
