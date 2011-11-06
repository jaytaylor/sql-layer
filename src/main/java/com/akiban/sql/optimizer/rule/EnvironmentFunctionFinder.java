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

import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionComposerWithBindingPosition;
import com.akiban.server.expression.ExpressionRegistry;
import com.akiban.server.expression.ExpressionType;
import static com.akiban.server.expression.std.EnvironmentExpression.*;

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

    public static final WhiteboardMarker<List<EnvironmentValue>> MARKER = 
        new DefaultWhiteboardMarker<List<EnvironmentValue>>();

    @Override
    public void apply(PlanContext planContext) {
        SchemaRulesContext rulesContext = (SchemaRulesContext)
            planContext.getRulesContext();
        Rewriter r = new Rewriter(rulesContext.getExpressionRegistry());
        planContext.getPlan().accept(r);
        if (r.environmentValues != null)
            planContext.putWhiteboard(MARKER, r.environmentValues);
    }

    static class Rewriter implements PlanVisitor, ExpressionRewriteVisitor {
        List<EnvironmentValue> environmentValues = null;
        ExpressionRegistry expressionRegistry;

        public Rewriter(ExpressionRegistry expressionRegistry) {
            this.expressionRegistry = expressionRegistry;
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
            ExpressionComposer composer;
            try {
                composer = expressionRegistry.composer(func.getFunction());
            }
            catch (NoSuchFunctionException ex) {
                return func;
            }
            if (!(composer instanceof ExpressionComposerWithBindingPosition))
                return func;
            
            // Needs bindings; figure out how.
            if (composer instanceof EnvironmentComposer) {
                EnvironmentValue environmentValue = ((EnvironmentComposer)composer).getEnvironmentValue();
                if (environmentValues == null)
                    environmentValues = new ArrayList<EnvironmentValue>();
                int bindingPosition = environmentValues.indexOf(environmentValue);
                if (bindingPosition < 0) {
                    bindingPosition = environmentValues.size();
                    environmentValues.add(environmentValue);
                }
                return new EnvironmentFunctionExpression(func.getFunction(),
                                                         bindingPosition,
                                                         func.getSQLtype(),
                                                         func.getAkType(),
                                                         func.getSQLsource());
            }
            else {
                throw new UnsupportedSQLException("Needs bindings but know how",
                                                  func.getSQLsource());
            }
        }
    }
}
