/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
        binder.setFunctionDefined(new AISBinder.FunctionDefined() {
                @Override
                public boolean isDefined(String name) {
                    return (getFunctionsRegistry().getFunctionKind(name) != null);
                }
            });
    }

    @Override
    protected void initT3Registry(T3RegistryService overloadResolver) {
        super.initT3Registry(overloadResolver);
        typeComputer.setUseComposers(false);
        binder.setFunctionDefined(new AISBinder.FunctionDefined() {
                @Override
                public boolean isDefined(String name) {
                    return (getT3Registry().getFunctionKind(name) != null);
                }
            });
    }

    @Override
    protected void initCostEstimator(CostEstimator costEstimator, boolean usePValues) {
        super.initCostEstimator(costEstimator, usePValues);

        List<BaseRule> rules = usePValues ? DEFAULT_RULES_NEWTYPES : DEFAULT_RULES_OLDTYPES;
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
