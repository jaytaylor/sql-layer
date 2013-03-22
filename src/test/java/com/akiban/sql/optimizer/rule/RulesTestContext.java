
package com.akiban.sql.optimizer.rule;

import com.akiban.server.t3expressions.T3RegistryServiceImpl;
import com.akiban.sql.optimizer.OptimizerTestBase;
import com.akiban.sql.optimizer.rule.cost.TestCostEstimator;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.service.functions.FunctionsRegistryImpl;

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
        context.initFunctionsRegistry(new FunctionsRegistryImpl());
        T3RegistryServiceImpl t3Registry = new T3RegistryServiceImpl();
        t3Registry.start();
        context.initT3Registry(t3Registry);
        context.initCostEstimator(new TestCostEstimator(ais, context.getSchema(), 
                                                        statsFile, statsIgnoreMissingIndexes,
                                                        properties), false);
        context.initDone();
        return context;
    }

    @Override
    public String getDefaultSchemaName() {
        return OptimizerTestBase.DEFAULT_SCHEMA;
    }

}
