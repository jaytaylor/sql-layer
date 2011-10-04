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

import com.akiban.sql.optimizer.plan.*;

import com.akiban.server.expression.std.Comparison;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExpressionCompactor extends BaseRule 
                                 implements PlanVisitor, ExpressionRewriteVisitor
{
    private static final Logger logger = LoggerFactory.getLogger(ExpressionCompactor.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        plan.getPlan().accept(this);
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
        return true;
    }

    @Override
    public ExpressionNode visit(ExpressionNode expr) {
        if (expr instanceof AnyCondition)
            return anyCondition((AnyCondition)expr);
        return expr;
    }

    protected ExpressionNode anyCondition(AnyCondition any) {
        Subquery subquery = any.getSubquery();
        PlanNode input = subquery.getInput();
        if (!(input instanceof Project))
            return any;
        Project project = (Project)input;
        input = project.getInput();
        if (!(input instanceof ExpressionsSource))
            return any;
        ExpressionsSource esource = (ExpressionsSource)input;
        if (project.getFields().size() != 1)
            return any;
        ExpressionNode cond = project.getFields().get(0);
        if (!(cond instanceof ComparisonCondition))
            return any;
        ComparisonCondition comp = (ComparisonCondition)cond;
        if (!(comp.getRight().isColumn() &&
              (comp.getOperation() == Comparison.EQ) &&
              (((ColumnExpression)comp.getRight()).getTable() == esource)))
            return any;
        List<List<ExpressionNode>> rows = esource.getExpressions();
        List<ExpressionNode> expressions = new ArrayList<ExpressionNode>(rows.size());
        for (List<ExpressionNode> row : rows) {
            if (row.size() != 1)
                return any;
            expressions.add(row.get(0));
        }
        if (expressions.isEmpty()) 
            return any;
        ExpressionNode operand = comp.getLeft();
        return new InListCondition(operand, expressions,
                                   any.getSQLtype(), any.getSQLsource());
    }

}
