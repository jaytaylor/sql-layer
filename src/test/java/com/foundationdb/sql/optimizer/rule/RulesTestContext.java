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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.server.expressions.T3RegistryServiceImpl;
import com.foundationdb.sql.optimizer.OptimizerTestBase;
import com.foundationdb.sql.optimizer.rule.cost.TestCostEstimator;

import com.foundationdb.ais.model.AkibanInformationSchema;

import java.util.List;
import java.util.Properties;
import java.io.File;

public class RulesTestContext extends SchemaRulesContext
{
    protected RulesTestContext() {
    }

    public static RulesTestContext create(AkibanInformationSchema ais,
                                          File statsFile, boolean statsIgnoreMissingIndexes,
                                          List<? extends BaseRule> rules, 
                                          Properties properties)
            throws Exception {
        RulesTestContext context = new RulesTestContext();
        context.initProperties(properties);
        context.initRules(rules);
        RulesTestHelper.ensureRowDefs(ais);
        context.initAIS(ais);
        T3RegistryServiceImpl t3Registry = new T3RegistryServiceImpl();
        t3Registry.start();
        context.initFunctionsRegistry(t3Registry);
        context.initT3Registry(t3Registry);
        context.initCostEstimator(new TestCostEstimator(ais, context.getSchema(), 
                                                        statsFile, statsIgnoreMissingIndexes,
                                                        properties));
        context.initPipelineConfiguration(new PipelineConfiguration());
        context.initDone();
        return context;
    }

    @Override
    public String getDefaultSchemaName() {
        return OptimizerTestBase.DEFAULT_SCHEMA;
    }

}
