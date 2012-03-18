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

package com.akiban.sql.optimizer;

import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.sql.optimizer.plan.AST;
import com.akiban.sql.optimizer.plan.BasePlannable;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.rule.CostEstimator;
import com.akiban.sql.optimizer.rule.SchemaRulesContext;
import static com.akiban.sql.optimizer.rule.DefaultRules.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.compiler.BooleanNormalizer;
import com.akiban.sql.parser.DMLStatementNode;
import com.akiban.sql.parser.NodeFactory;
import com.akiban.sql.parser.ParameterNode;
import com.akiban.sql.parser.SQLParser;
import com.akiban.sql.parser.SQLParserContext;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.server.error.SQLParserInternalException;

import com.akiban.ais.model.AkibanInformationSchema;

import java.util.List;
import java.util.Properties;

/**
 * Compile SQL statements into operator trees.
 */ 
// TODO: Temporary name during transition.
public class OperatorCompiler extends SchemaRulesContext
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected AISTypeComputer typeComputer;
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
        nodeFactory = parserContext.getNodeFactory();
        parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
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
    protected void initCostEstimator(CostEstimator costEstimator) {
        super.initCostEstimator(costEstimator);
        initRules((costEstimator != null) ? DEFAULT_RULES_CBO : DEFAULT_RULES);
    }

    @Override
    protected void initDone() {
        super.initDone();
        assert (parserContext != null) : "initParser() not called";
    }

    public void addView(ViewDefinition view) throws StandardException {
        binder.addView(view);
    }

    /** Compile a statement into an operator tree. */
    public BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params) {
        return compile(stmt, params, new PlanContext(this));
    }

    protected BasePlannable compile(DMLStatementNode stmt, List<ParameterNode> params,
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
            typeComputer.compute(stmt);
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

}
