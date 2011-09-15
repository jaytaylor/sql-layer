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

import java.util.*;

/** Evaluate as much as possible at generate time. */
public class FoldConstants extends BaseRule implements ExpressionRewriteVisitor
{
    @Override
    public PlanNode apply(PlanNode plan) {
        boolean changed;
        do {
            changed = foldConstants(plan);
        } while (changed);
        return plan;
    }

    protected boolean foldConstants(PlanNode plan) {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(ExpressionNode expr) {
        // This assumes that there is no particular advantage to
        // figuring out the whole expression tree of functions and
        // constants and passing it to eval as a whole, rather than
        // doing it piece-by-piece.
        return true;
    }

    @Override
    public ExpressionNode visit(ExpressionNode expr) {
        if (expr instanceof ComparisonCondition)
            return comparisonCondition((ComparisonCondition)expr);
        else if (expr instanceof CastExpression)
            return castExpression((CastExpression)expr);
        else if (expr instanceof FunctionExpression)
            return functionExpression((FunctionExpression)expr);
        else if (expr instanceof IfElseExpression)
            return ifElseExpression((IfElseExpression)expr);
        else if (expr instanceof ColumnExpression)
            return columnExpression((ColumnExpression)expr);
        else if (expr instanceof SubqueryExpression)
            return subqueryExpression((SubqueryExpression)expr);
        else if (expr instanceof SubqueryCondition)
            return subqueryCondition((SubqueryCondition)expr);
        else
            return expr;
    }

    protected ExpressionNode comparisonCondition(ComparisonCondition cond) {
        Constantness lc = isConstant(cond.getLeft());
        Constantness rc = isConstant(cond.getRight());
        if ((lc != Constantness.VARIABLE) && (rc != Constantness.VARIABLE))
            return evalNow(cond);
        if ((lc == Constantness.NULL) || (rc == Constantness.NULL))
            return new BooleanConstantExpression(null, 
                                                 cond.getSQLtype(), 
                                                 cond.getSQLsource());
        return cond;
    }

    protected ExpressionNode castExpression(CastExpression cast) {
        Constantness c = isConstant(cast.getOperand());
        if (c != Constantness.VARIABLE)
            return evalNow(cast);
        return cast;
    }

    protected ExpressionNode functionExpression(FunctionExpression fun) {
        String fname = fun.getFunction();
        if ("isNullOp".equals(fname))
            return isNullExpression(fun);
        else if ("COALESCE".equals(fname))
            return coalesceExpression(fun);

        boolean allConstant = true, anyNull = false;
        for (ExpressionNode operand : fun.getOperands()) {
            switch (isConstant(operand)) {
            case NULL:
                anyNull = true;
                /* falls through */
            case VARIABLE:
                allConstant = false;
                break;
            }
        }
        if (allConstant && !isVolatile(fun))
            return evalNow(fun);
        // All the functions that treat NULL specially are caught before we get here.
        if (anyNull)
            return new BooleanConstantExpression(null, 
                                                 fun.getSQLtype(), 
                                                 fun.getSQLsource());
        return fun;
    }

    protected boolean isVolatile(FunctionExpression fun) {
        // TODO: Nice to get this from some functions repository
        // associated with their implementations.
        String fname = fun.getFunction();
        return ("currentDate".equals(fname) ||
                "currentTime".equals(fname) ||
                "currentTimestamp".equals(fname) ||
                "RAND".equals(fname));
    }

    protected ExpressionNode isNullExpression(FunctionExpression fun) {
        ExpressionNode operand = fun.getOperands().get(0);
        if (isConstant(operand) != Constantness.VARIABLE)
            return evalNow(fun);
        // pkey IS NULL is FALSE, for instance.
        if ((operand.getSQLtype() != null) &&
            !operand.getSQLtype().isNullable())
            return new BooleanConstantExpression(Boolean.FALSE, 
                                                 fun.getSQLtype(), 
                                                 fun.getSQLsource());
        return fun;
    }

    protected ExpressionNode coalesceExpression(FunctionExpression fun) {
        // Don't need all the operands to make progress.
        List<ExpressionNode> operands = fun.getOperands();
        int i = 0;
        while (i < operands.size()) {
            ExpressionNode operand = operands.get(i);
            Constantness c = isConstant(operand);
            if (c == Constantness.NULL) {
                operands.remove(i);
                continue;
            }
            if (c == Constantness.CONSTANT) {
                // If the first arg is a not-null constant, that's the answer.
                if (i == 0) return operand;
            }
            i++;
        }
        if (operands.isEmpty())
            return new BooleanConstantExpression(null, 
                                                 fun.getSQLtype(), 
                                                 fun.getSQLsource());
        return fun;
    }

    protected ExpressionNode ifElseExpression(IfElseExpression cond) {
        Constantness c = isConstant(cond.getTestCondition());
        if (c == Constantness.VARIABLE)
            return cond;
        if (((ConstantExpression)cond.getTestCondition()).getValue() == Boolean.TRUE)
            return cond.getThenExpression();
        else
            return cond.getElseExpression();
    }

    protected ExpressionNode columnExpression(ColumnExpression col) {
        return col;
    }

    protected ExpressionNode subqueryExpression(SubqueryExpression expr) {
        return expr;
    }

    protected ExpressionNode subqueryCondition(SubqueryCondition expr) {
        return expr;
    }
    
    protected static enum Constantness { VARIABLE, CONSTANT, NULL }

    protected Constantness isConstant(ExpressionNode expr) {
        if (expr.isConstant())
            return ((((ConstantExpression)expr).getValue() == null) ? 
                    Constantness.NULL :
                    Constantness.CONSTANT);
        else 
            return Constantness.VARIABLE;
    }

    protected ExpressionNode evalNow(ExpressionNode expr) {
        return expr;
    }

}
