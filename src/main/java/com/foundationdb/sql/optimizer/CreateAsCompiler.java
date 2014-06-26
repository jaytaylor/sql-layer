package com.foundationdb.sql.optimizer;

import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.sql.embedded.EmbeddedOperatorCompiler;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.CreateTableAsRules;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.sql.server.ServerSession;

import java.util.List;

import static com.foundationdb.sql.optimizer.rule.DefaultRules.DEFAULT_RULES;


public class CreateAsCompiler extends EmbeddedOperatorCompiler {

    boolean removeTableSources;

    public CreateAsCompiler(ServerSession server, StoreAdapter adapter, boolean removeTableSources) {
        this.initServer(server, adapter);
        this.initDone();

        if(removeTableSources) {
            List<BaseRule> newRules = DEFAULT_RULES;
            CreateTableAsRules newRule = new CreateTableAsRules();
            newRules.add(newRule);
            initRules(newRules);
        }

        this.removeTableSources = removeTableSources;

    }

    @Override
    protected void initCostEstimator(CostEstimator costEstimator) {
        super.initCostEstimator(costEstimator);
        List<BaseRule> rules = DEFAULT_RULES;
        if(removeTableSources) {
            CreateTableAsRules newRule = new CreateTableAsRules();
            rules.add(newRule);
        }
        initRules(rules);
    }



}

