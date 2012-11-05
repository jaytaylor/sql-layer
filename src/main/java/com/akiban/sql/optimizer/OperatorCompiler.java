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

package com.akiban.sql.optimizer;

import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.rule.BaseRule;
import com.akiban.sql.optimizer.rule.PlanContext;
import com.akiban.sql.optimizer.rule.SchemaRulesContext;
import com.akiban.sql.optimizer.rule.cost.CostEstimator;
import static com.akiban.sql.optimizer.rule.DefaultRules.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.NodeFactory;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserContext;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.server.error.SQLParserInternalException;
import com.akiban.server.service.functions.FunctionsRegistry;

import com.akiban.sql.IncomparableException;
import java.util.List;

/**
 * Compile SQL statements into operator trees.
 */ 
public class OperatorCompiler extends SchemaRulesContext
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected FunctionsTypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected DistinctEliminator distinctEliminator;

    protected OperatorCompiler() {
    }

    protected void initAIS(AkibanInformationSchema ais, String defaultSchemaName) {
        initAIS(ais);
        binder = new AISBinder(ais, defaultSchemaName);
    }

    protected void initParser(SQLParser parser) {
        parserContext = parser;
        BindingNodeFactory.wrap(parser);
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        distinctEliminator = new DistinctEliminator(parser);
    }

    @Override
    protected void initFunctionsRegistry(FunctionsRegistry functionsRegistry) {
        super.initFunctionsRegistry(functionsRegistry);
        typeComputer = new FunctionsTypeComputer(functionsRegistry);
    }

    @Override
    protected void initT3Registry(T3RegistryService overloadResolver) {
        super.initT3Registry(overloadResolver);
        typeComputer.setUseComposers(false);
    }

    @Override
    protected void initCostEstimator(CostEstimator costEstimator, boolean usePValues) {
        super.initCostEstimator(costEstimator, usePValues);

        List<BaseRule> rules;
        if (costEstimator != null) {
            rules = usePValues ? DEFAULT_RULES_NEWTYPES : DEFAULT_RULES_CBO;
        }
        else {
            rules = DEFAULT_RULES_OLD;
        }
        initRules(rules);
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (parserContext != null) : "initParser() not called";
    }

    public boolean usesPValues() {
        return rulesAre(DEFAULT_RULES_NEWTYPES);
    }

    /** Compile a statement into an operator tree. */
    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params) {
        return compile(stmt, params, new PlanContext(this));
    }

    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params,
                                 PlanContext plan) {
        stmt = bindAndTransform(stmt); // Get into standard form.
        plan.setPlan(new AST(stmt, params));
        applyRules(plan);
        return (BasePlannable)plan.getPlan();
    }

    /** Apply AST-level transformations before rules. */
    protected DMLStatementNode bindAndTransform(DMLStatementNode stmt)  {
        try {
            binder.bind(stmt);
            stmt = (DMLStatementNode)booleanNormalizer.normalize(stmt);
            try
            {
                typeComputer.compute(stmt);
                
            }
            catch (IncomparableException e) // catch this and let the resolvers decide
            {
                if (!this.usesPValues())
                    throw new SQLParserInternalException(e);  
                
            }
                    
            stmt = subqueryFlattener.flatten(stmt);
            // TODO: Temporary for safety.
            if (Boolean.parseBoolean(getProperty("eliminate-distincts", "true")))
                stmt = distinctEliminator.eliminate(stmt);
            return stmt;
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    @Override
    public String getDefaultSchemaName() {
        return binder.getDefaultSchemaName();
    }

}
