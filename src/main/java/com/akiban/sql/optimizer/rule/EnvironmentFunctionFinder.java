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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.error.NoSuchFunctionException;

import com.akiban.sql.optimizer.plan.*;
import static com.akiban.sql.optimizer.plan.PlanContext.*;

import com.akiban.server.expression.EnvironmentExpressionFactory;
import com.akiban.server.expression.EnvironmentExpressionSetting;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.service.functions.FunctionsRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Replace normal function calls to functions that access the
 * environment and assign them a binding position.
 */
public class EnvironmentFunctionFinder extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(EnvironmentFunctionFinder.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public static final WhiteboardMarker<List<EnvironmentExpressionSetting>> MARKER = 
        new DefaultWhiteboardMarker<List<EnvironmentExpressionSetting>>();

    /** Recover the {@link EnvironmentExpressionSetting} list put on the whiteboard when loaded. */
    public static List<EnvironmentExpressionSetting> getEnvironmentSettings(PlanContext plan) {
        return plan.getWhiteboard(MARKER);
    }

    @Override
    public void apply(PlanContext planContext) {
        SchemaRulesContext rulesContext = (SchemaRulesContext)
            planContext.getRulesContext();
        Rewriter r = new Rewriter(rulesContext.getFunctionsRegistry());
        planContext.getPlan().accept(r);
        if (r.environmentSettings != null)
            planContext.putWhiteboard(MARKER, r.environmentSettings);
    }

    static class Rewriter implements PlanVisitor, ExpressionRewriteVisitor {
        List<EnvironmentExpressionSetting> environmentSettings = null;
        FunctionsRegistry functionsRegistry;

        public Rewriter(FunctionsRegistry functionsRegistry) {
            this.functionsRegistry = functionsRegistry;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }
        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }
        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode expr) {
            return false;
        }

        @Override
        public ExpressionNode visit(ExpressionNode expr) {
            if (expr instanceof FunctionExpression)
                return functionExpression((FunctionExpression)expr);
            else
                return expr;
        }

        protected ExpressionNode functionExpression(FunctionExpression func) {
            if (!func.getOperands().isEmpty())
                return func;
            EnvironmentExpressionFactory factory;
            try {
                factory = functionsRegistry.environment(func.getFunction());
            }
            catch (NoSuchFunctionException ex) {
                return func;
            }
            EnvironmentExpressionSetting setting = factory.environmentSetting();
            if (environmentSettings == null)
                environmentSettings = new ArrayList<EnvironmentExpressionSetting>();
            int bindingPosition = environmentSettings.indexOf(setting);
            if (bindingPosition < 0) {
                bindingPosition = environmentSettings.size();
                environmentSettings.add(setting);
            }
            return new EnvironmentFunctionExpression(func.getFunction(),
                                                     bindingPosition,
                                                     func.getSQLtype(),
                                                     func.getAkType(),
                                                     func.getSQLsource());
        }
    }
}
