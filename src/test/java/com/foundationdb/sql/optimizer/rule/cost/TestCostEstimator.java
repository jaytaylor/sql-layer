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

package com.foundationdb.sql.optimizer.rule.cost;

import com.foundationdb.ais.model.*;
import com.foundationdb.sql.optimizer.OptimizerTestBase;

import com.foundationdb.qp.rowtype.Schema;

import com.foundationdb.server.collation.TestKeyCreator;
import com.foundationdb.server.store.statistics.IndexStatistics;
import com.foundationdb.server.store.statistics.IndexStatisticsYamlLoader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TestCostEstimator extends CostEstimator
{
    private final AkibanInformationSchema ais;
    private final Map<Index,IndexStatistics> stats;

    public static class TestCostModelFactory implements CostModelFactory {
        @Override
        public CostModel newCostModel(Schema schema, TableRowCounts tableRowCounts) {
            // NOTE: For now, we use the Persistit model since that is how all the
            // existing tests were computed.
            return new PersistitCostModel(schema, tableRowCounts);
        }
    }

    public TestCostEstimator(AkibanInformationSchema ais, Schema schema, 
                             File statsFile, boolean statsIgnoreMissingIndexes,
                             Properties properties)
            throws IOException {
        super(schema, properties, new TestKeyCreator(schema), new TestCostModelFactory());
        this.ais = ais;
        if (statsFile == null)
            stats = Collections.<Index,IndexStatistics>emptyMap();
        else
            stats = new IndexStatisticsYamlLoader(ais, OptimizerTestBase.DEFAULT_SCHEMA, new TestKeyCreator(schema))
                .load(statsFile, statsIgnoreMissingIndexes);
    }

    @Override
    public IndexStatistics getIndexStatistics(Index index) {
        return stats.get(index);
    }

}
