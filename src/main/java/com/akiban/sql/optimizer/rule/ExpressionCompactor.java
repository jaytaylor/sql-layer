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

package com.akiban.sql.optimizer.rule;

import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression.Operation;

import com.akiban.server.expression.std.Comparison;

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
        new Compactor().compact(plan);
    }

    static class Compactor implements PlanVisitor, ExpressionRewriteVisitor {
        Collection<BasePlanWithInput> toRemove = new ArrayList<BasePlanWithInput>();

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
            if (expr instanceof IfElseExpression)
                compactConditions(null, ((IfElseExpression)expr).getTestConditions());
            return expr;
        }
    }

    protected static void compactConditions(PlanNode node, ConditionList conditions) {
        if (conditions.size() <= 1)
            return;
        
        Map<ColumnSource,List<ConditionExpression>> byTable = 
            new HashMap<ColumnSource,List<ConditionExpression>>();
        if (node != null) {
            ConditionDependencyAnalyzer dependencies = 
                new ConditionDependencyAnalyzer(node);
            for (ConditionExpression condition : conditions) {
                ColumnSource table = dependencies.analyze(condition);
                List<ConditionExpression> entry = byTable.get(table);
                if (entry == null) {
                    entry = new ArrayList<ConditionExpression>();
                    byTable.put(table, entry);
                }
                entry.add(condition);
            }
        }
        else {
            byTable.put(null, new ArrayList<ConditionExpression>(conditions));
        }
        conditions.clear();
        List<ColumnSource> tables = new ArrayList<ColumnSource>(byTable.keySet());
        Collections.sort(tables, tableSourceById);
        for (ColumnSource table : tables) {
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
                    condition.setPreptimeValue(new TPreptimeValue(AkBool.INSTANCE.instance()));
                }
            }
            conditions.add(condition);
        }
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
