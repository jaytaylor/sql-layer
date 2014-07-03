package com.foundationdb.sql.optimizer;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.qp.operator.StoreAdapter;
import com.foundationdb.sql.embedded.EmbeddedOperatorCompiler;
import com.foundationdb.sql.optimizer.rule.ASTStatementLoader;
import com.foundationdb.sql.optimizer.rule.BaseRule;
import com.foundationdb.sql.optimizer.rule.CreateTableAsRules;
import com.foundationdb.sql.optimizer.rule.HalloweenRecognizer;
import com.foundationdb.sql.optimizer.rule.cost.CostEstimator;

import com.foundationdb.sql.server.ServerSession;

import java.util.ArrayList;
import java.util.List;

import static com.foundationdb.sql.optimizer.rule.DefaultRules.DEFAULT_RULES;


public class CreateAsCompiler extends EmbeddedOperatorCompiler {

    boolean removeTableSources;

    public CreateAsCompiler(ServerSession server, StoreAdapter adapter, boolean removeTableSources, AkibanInformationSchema ais) {
        initProperties(server.getCompilerProperties());
        initAIS(ais, server.getDefaultSchemaName());
        initParser(server.getParser());
        initCostEstimator(server.costEstimator(this, adapter));
        initPipelineConfiguration(server.getPipelineConfiguration());
        initTypesRegistry(server.typesRegistryService());
        initTypesTranslator(server.typesTranslator());
        server.getBinderContext().setBinderAndTypeComputer(binder, typeComputer);
        server.setAttribute("compiler", this);
        this.initDone();

        if(removeTableSources) {
            List<BaseRule> newRules = new ArrayList<>();
            for(BaseRule rule : DEFAULT_RULES){
                newRules.add(rule);
                if(rule instanceof ASTStatementLoader){
                    CreateTableAsRules newRule = new CreateTableAsRules();
                    newRules.add(newRule);
                }
            }
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

    @Override
    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        super.initAIS(ais, defaultSchemaName);
        binder.setAllowSubqueryMultipleColumns(true);
    }



}

