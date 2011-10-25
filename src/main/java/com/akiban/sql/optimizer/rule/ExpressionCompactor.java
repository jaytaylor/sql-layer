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
import com.akiban.sql.optimizer.plan.BooleanOperationExpression.Operation;

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
        if (n instanceof Select)
            compactConditions(((Select)n).getConditions());
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
        if (expr instanceof IfElseExpression)
            compactConditions(((IfElseExpression)expr).getTestConditions());
        return expr;
    }

    protected void compactConditions(ConditionList conditions) {
        if (conditions.size() <= 1)
            return;

        Map<TableSource,List<ConditionExpression>> byTable = 
            new HashMap<TableSource,List<ConditionExpression>>();
        for (ConditionExpression condition : conditions) {
            TableSource table = SelectPreponer.getSingleTableConditionTable(condition);
            List<ConditionExpression> entry = byTable.get(table);
            if (entry == null) {
                entry = new ArrayList<ConditionExpression>();
                byTable.put(table, entry);
            }
            entry.add(condition);
        }
        conditions.clear();
        List<TableSource> tables = new ArrayList<TableSource>(byTable.keySet());
        Collections.sort(tables, tableSourceById);
        for (TableSource table : tables) {
            List<ConditionExpression> entry = byTable.get(table);
            ConditionExpression condition;
            int size = entry.size();
            if (size == 1)
                condition = entry.get(0);
            else {
                Collections.sort(entry, conditionBySelectivity);
                condition = entry.get(--size);
                while (size > 0) {
                    condition = new BooleanOperationExpression(Operation.AND,
                                                               entry.get(--size),
                                                               condition,
                                                               null, null);
                }
            }
            conditions.add(condition);
        }
    }

    static final Comparator<TableSource> tableSourceById = 
        new Comparator<TableSource>() {
        @Override
        public int compare(TableSource t1, TableSource t2) {
            if (t1 == null) {
                if (t2 == null)
                    return 0;
                else
                    return +1;
            }
            else if (t2 == null)
                return -1;
            else
                return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
        }
    };

    static final Comparator<ConditionExpression> conditionBySelectivity = 
        new Comparator<ConditionExpression>() {
        @Override
        public int compare(ConditionExpression c1, ConditionExpression c2) {
            return conditionSelectivity(c1) - conditionSelectivity(c2);
        }
    };

    protected static int conditionSelectivity(ConditionExpression condition) {
        if (condition instanceof ComparisonCondition) {
            switch (((ComparisonCondition)condition).getOperation()) {
            case EQ:
                return 1;
            case NE:
                return 3;
            default:
                return 2;
            }
        }
        else if (condition instanceof FunctionExpression) {
            String fname = ((FunctionExpression)condition).getFunction();
            if ("isNull".equals(fname))
                return 1;
        }
        else if (condition instanceof SubqueryExpression)
            return 10;
        return 5;
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
