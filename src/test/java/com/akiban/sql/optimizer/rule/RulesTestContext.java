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
