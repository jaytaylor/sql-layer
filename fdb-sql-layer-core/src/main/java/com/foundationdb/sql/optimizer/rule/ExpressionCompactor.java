/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.sql.optimizer.rule;

import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.optimizer.plan.BooleanOperationExpression.Operation;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.texpressions.Comparison;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExpressionCompactor extends BaseRule 
{
    private static final Logger logger = LoggerFactory.getLogger(ExpressionCompactor.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        new Compactor((SchemaRulesContext)plan.getRulesContext()).compact(plan);
    }

    static class Compactor implements PlanVisitor, ExpressionRewriteVisitor {
        SchemaRulesContext rulesContext;
        Collection<BasePlanWithInput> toRemove = new ArrayList<>();

        public Compactor(SchemaRulesContext rulesContext) {
            this.rulesContext = rulesContext;
        }

        public void compact(PlanContext plan) {
            plan.accept(this);
            for (BasePlanWithInput n : toRemove) {
                n.getOutput().replaceInput(n, n.getInput());
            }
        }

        @Override
            public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
            public boolean visitLeave(PlanNode n) {
            if (n instanceof Select) {
                Select select = (Select)n;
                ConditionList conditions = select.getConditions();
                compactConditions(n, conditions);
                if (conditions.isEmpty())
                    toRemove.add(select);
            }
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

        protected void compactConditions(PlanNode node, ConditionList conditions) {
            if (conditions.size() <= 1)
                return;

            Map<ColumnSource,List<ConditionExpression>> byTable = 
                new HashMap<>();
            if (node != null) {
                ConditionDependencyAnalyzer dependencies = 
                    new ConditionDependencyAnalyzer(node);
                for (ConditionExpression condition : conditions) {
                    ColumnSource table = dependencies.analyze(condition);
                    List<ConditionExpression> entry = byTable.get(table);
                    if (entry == null) {
                        entry = new ArrayList<>();
                        byTable.put(table, entry);
                    }
                    entry.add(condition);
                }
            }
            else {
                byTable.put(null, new ArrayList<>(conditions));
            }
            conditions.clear();
            List<ColumnSource> tables = new ArrayList<>(byTable.keySet());
            Collections.sort(tables, tableSourceById);
            for (ColumnSource table : tables) {
                List<ConditionExpression> entry = byTable.get(table);
                ConditionExpression condition;
                int size = entry.size();
                if (size == 1) {
                    condition = entry.get(0);
                }
                else {
                    Collections.sort(entry, conditionBySelectivity);
                    condition = entry.get(--size);
                    boolean nullable = isNullable(condition);
                    while (size > 0) {
                        ConditionExpression left = entry.get(--size);
                        nullable |= isNullable(left);
                        DataTypeDescriptor sqlType = new DataTypeDescriptor (TypeId.BOOLEAN_ID, nullable);
                        TInstance type = rulesContext.getTypesTranslator().typeForSQLType(sqlType);
                        condition = new BooleanOperationExpression(Operation.AND,
                                                                   left,
                                                                   condition,
                                                                   sqlType,
                                                                   null,
                                type);
                    }
                }
                conditions.add(condition);
            }
        }

    }

    private static boolean isNullable(ExpressionNode condition) {
        return condition.getPreptimeValue().isNullable();
    }

    static final Comparator<ColumnSource> tableSourceById = 
        new Comparator<ColumnSource>() {
        @Override
        public int compare(ColumnSource t1, ColumnSource t2) {
            if (!(t1 instanceof TableSource)) {
                if (!(t2 instanceof TableSource))
                    return 0;
                else
                    return +1;
            }
            else if (!(t2 instanceof TableSource))
                return -1;
            else
                return ((TableSource)t1).getTable().getTable().getTableId().compareTo(((TableSource)t2).getTable().getTable().getTableId());
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
        
    protected static ExpressionNode anyCondition(AnyCondition any) {
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
        List<ExpressionNode> expressions = new ArrayList<>(rows.size());
        for (List<ExpressionNode> row : rows) {
            if (row.size() != 1)
                return any;
            expressions.add(row.get(0));
        }
        if (expressions.isEmpty()) 
            return any;
        ExpressionNode operand = comp.getLeft();
        InListCondition inList = new InListCondition(operand, expressions,
                                                     any.getSQLtype(), any.getSQLsource(), any.getType());

        inList.setComparison(comp);
        return inList;
    }

}
