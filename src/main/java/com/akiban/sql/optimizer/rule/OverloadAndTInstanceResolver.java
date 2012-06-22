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

import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.T3ScalarsRegistery;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.sql.optimizer.plan.AggregateFunctionExpression;
import com.akiban.sql.optimizer.plan.AnyCondition;
import com.akiban.sql.optimizer.plan.BooleanConstantExpression;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression;
import com.akiban.sql.optimizer.plan.CastExpression;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExistsCondition;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionRewriteVisitor;
import com.akiban.sql.optimizer.plan.ExpressionVisitor;
import com.akiban.sql.optimizer.plan.FunctionExpression;
import com.akiban.sql.optimizer.plan.IfElseExpression;
import com.akiban.sql.optimizer.plan.InListCondition;
import com.akiban.sql.optimizer.plan.ParameterCondition;
import com.akiban.sql.optimizer.plan.ParameterExpression;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.SubqueryResultSetExpression;
import com.akiban.sql.optimizer.plan.SubqueryValueExpression;
import com.akiban.sql.optimizer.rule.ConstantFolder.Folder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OverloadAndTInstanceResolver extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(OverloadAndTInstanceResolver.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        new ResolvingVistor(plan).resolve(plan.getPlan());
    }

    static class ResolvingVistor implements PlanVisitor, ExpressionRewriteVisitor {

        private Folder folder;
        private OverloadResolver resolver;
        private T3ScalarsRegistery registry;


        ResolvingVistor(PlanContext context) {
            folder = new Folder(context);
        }

        public void resolve(PlanNode root) {
            root.accept(this);
        }

        @Override
        public boolean visitChildrenFirst(ExpressionNode n) {
            return true;
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
        public ExpressionNode visit(ExpressionNode n) {
            if (n instanceof CastExpression)
                n = handleCastExpression((CastExpression) n);
            else if (n instanceof FunctionExpression)
                n = handleFunctionExpression((FunctionExpression) n);
            else if (n instanceof IfElseExpression)
                n = handleIfElseExpression((IfElseExpression) n);
            else if (n instanceof AggregateFunctionExpression)
                n = handleAggregateFunctionExpression((AggregateFunctionExpression) n);
            else if (n instanceof ExistsCondition)
                n = handleExistsCondition((ExistsCondition) n);
            else if (n instanceof SubqueryValueExpression)
                n = handleSubqueryValueExpression((SubqueryValueExpression) n);
            else if (n instanceof SubqueryResultSetExpression)
                n = handleSubqueryResultSetExpression((SubqueryResultSetExpression) n);
            else if (n instanceof AnyCondition)
                n = handleAnyCondition((AnyCondition) n);
            else if (n instanceof ComparisonCondition)
                n = handleComparisonCondition((ComparisonCondition) n);
            else if (n instanceof ColumnExpression)
                n = handleColumnExpression((ColumnExpression) n);
            else if (n instanceof InListCondition)
                n = handleInListCondition((InListCondition) n);
            else if (n instanceof ParameterCondition)
                n = handleParameterCondition((ParameterCondition) n);
            else if (n instanceof ParameterExpression)
                n = handleParameterExpression((ParameterExpression) n);
            else if (n instanceof BooleanOperationExpression)
                n = handleBooleanOperationExpression((BooleanOperationExpression) n);
            else if (n instanceof BooleanConstantExpression)
                n = handleBooleanConstantExpression((BooleanConstantExpression) n);
            else if (n instanceof ConstantExpression)
                n = handleConstantExpression((ConstantExpression) n);
            else
                logger.warn("unrecognized ExpressionNode subclass: {}", n.getClass());
            return n;
        }

        ExpressionNode handleCastExpression(CastExpression expression) {
            //TODO
        }

        ExpressionNode handleFunctionExpression(FunctionExpression expression) {
            //TODO
        }

        ExpressionNode handleIfElseExpression(IfElseExpression expression) {
            ExpressionNode thenExpr = expression.getThenExpression();
            ExpressionNode elseExpr = expression.getElseExpression();

            TInstance thenType = thenExpr.getPreptimeValue().instance();
            TInstance elseType = elseExpr.getPreptimeValue().instance();

            TClass commonClass = registry.commonTClass(thenType.typeClass(), elseType.typeClass());
            thenExpr = castTo(thenExpr, commonClass);
            elseExpr = castTo(elseExpr, commonClass);
        }

        ExpressionNode handleAggregateFunctionExpression(AggregateFunctionExpression expression) {
            //TODO
        }

        ExpressionNode handleExistsCondition(ExistsCondition expression) {
            //TODO
        }

        ExpressionNode handleSubqueryValueExpression(SubqueryValueExpression expression) {
            //TODO
        }

        ExpressionNode handleSubqueryResultSetExpression(SubqueryResultSetExpression expression) {
            //TODO
        }

        ExpressionNode handleAnyCondition(AnyCondition expression) {
            //TODO
        }

        ExpressionNode handleComparisonCondition(ComparisonCondition expression) {
            //TODO
        }

        ExpressionNode handleColumnExpression(ColumnExpression expression) {
            //TODO
        }

        ExpressionNode handleInListCondition(InListCondition expression) {
            //TODO
        }

        ExpressionNode handleParameterCondition(ParameterCondition expression) {
            //TODO
        }

        ExpressionNode handleParameterExpression(ParameterExpression expression) {
            //TODO
        }

        ExpressionNode handleBooleanOperationExpression(BooleanOperationExpression expression) {
            //TODO
        }

        ExpressionNode handleBooleanConstantExpression(BooleanConstantExpression expression) {
            //TODO
        }

        ExpressionNode handleConstantExpression(ConstantExpression expression) {
            //TODO
        }

        private ExpressionNode castTo(ExpressionNode expression, TClass targetClass) {
            if (expression.getPreptimeValue().instance().typeClass().equals(targetClass))
                return expression;
            CastExpression result = null; // TODO
            assert result != null : "todo";
            return result;
        }

    }
}
