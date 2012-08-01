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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.ColumnContainer;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.OverloadResolver.OverloadResult;
import com.akiban.server.types3.LazyListBase;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.aktypes.AkBool;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueSources;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.AggregateFunctionExpression;
import com.akiban.sql.optimizer.plan.AggregateSource;
import com.akiban.sql.optimizer.plan.AnyCondition;
import com.akiban.sql.optimizer.plan.BooleanConstantExpression;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression;
import com.akiban.sql.optimizer.plan.CastExpression;
import com.akiban.sql.optimizer.plan.ColumnExpression;
import com.akiban.sql.optimizer.plan.ColumnSource;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExistsCondition;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.ExpressionRewriteVisitor;
import com.akiban.sql.optimizer.plan.ExpressionsSource;
import com.akiban.sql.optimizer.plan.FunctionExpression;
import com.akiban.sql.optimizer.plan.IfElseExpression;
import com.akiban.sql.optimizer.plan.InListCondition;
import com.akiban.sql.optimizer.plan.InsertStatement;
import com.akiban.sql.optimizer.plan.NullSource;
import com.akiban.sql.optimizer.plan.ParameterCondition;
import com.akiban.sql.optimizer.plan.ParameterExpression;
import com.akiban.sql.optimizer.plan.PlanContext;
import com.akiban.sql.optimizer.plan.PlanNode;
import com.akiban.sql.optimizer.plan.PlanVisitor;
import com.akiban.sql.optimizer.plan.Project;
import com.akiban.sql.optimizer.plan.ResultSet;
import com.akiban.sql.optimizer.plan.SubqueryResultSetExpression;
import com.akiban.sql.optimizer.plan.SubquerySource;
import com.akiban.sql.optimizer.plan.SubqueryValueExpression;
import com.akiban.sql.optimizer.plan.TableSource;
import com.akiban.sql.optimizer.plan.UpdateStatement;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;
import com.akiban.sql.optimizer.rule.ConstantFolder.NewFolder;
import com.akiban.sql.types.DataTypeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class OverloadAndTInstanceResolver extends BaseRule {
    private static final Logger logger = LoggerFactory.getLogger(OverloadAndTInstanceResolver.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    @Override
    public void apply(PlanContext plan) {
        new ResolvingVistor(plan).resolve(plan.getPlan());
        new TopLevelCastingVistor().apply(plan.getPlan());

    }

    static class ResolvingVistor implements PlanVisitor, ExpressionRewriteVisitor {

        private NewFolder folder;
        private OverloadResolver resolver;
        private QueryContext queryContext;

        ResolvingVistor(PlanContext context) {
            folder = new NewFolder(context);
            SchemaRulesContext src = (SchemaRulesContext)context.getRulesContext();
            resolver = src.getOverloadResolver();
            this.queryContext = context.getQueryContext();
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
            if (n instanceof ResultSet) {
                ResultSet rs = (ResultSet) n;
                for (ResultSet.ResultField field : rs.getFields()) {
                    DataTypeDescriptor sourceDtd = field.getSourceExpression().getSQLtype();
                    DataTypeDescriptor fieldDtd = field.getSQLtype();
                    if (!sourceDtd.equals(fieldDtd)) {
                        TPreptimeValue tpv = field.getSourceExpression().getPreptimeValue();
                        if (tpv != null) {
                            TInstance tInstance = tpv.instance();
                            if (tInstance != null) {
                                DataTypeDescriptor newDtd = tInstance.dataTypeDescriptor();
                                field.setSQLtype(newDtd);
                            }
                        }
                    }
                }
            }
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

            n = folder.foldConstants(n);
            // Set nullability of TInstance if it hasn't been given explicitly
            // At the same time, update the node's DataTypeDescriptor to match its TInstance
            TPreptimeValue tpv = n.getPreptimeValue();
            if (tpv != null) {
                TInstance tInstance = tpv.instance();
                if (tInstance != null) {
                    if (tInstance.nullability() == null)
                        tInstance.setNullable(n.getSQLtype().isNullable());
                    DataTypeDescriptor newDtd = tInstance.dataTypeDescriptor();
                    n.setSQLtype(newDtd);
                }
            }
            return n;
        }

        ExpressionNode handleCastExpression(CastExpression expression) {
            DataTypeDescriptor dtd = expression.getSQLtype();
            TInstance instance = TypesTranslation.toTInstance(dtd);
            expression.setPreptimeValue(new TPreptimeValue(instance));
            return expression;
        }

        ExpressionNode handleFunctionExpression(FunctionExpression expression) {
            List<ExpressionNode> operands = expression.getOperands();
            List<TPreptimeValue> operandClasses = new ArrayList<TPreptimeValue>(operands.size());
            for (ExpressionNode operand : operands)
                operandClasses.add(operand.getPreptimeValue());

            OverloadResult resolutionResult = resolver.get(expression.getFunction(), operandClasses);

            // cast operands
            for (int i = 0, operandsSize = operands.size(); i < operandsSize; i++) {

                ExpressionNode operand = castTo(operands.get(i), resolutionResult.getTypeClass(i));
                operands.set(i, operand);
            }

            TValidatedOverload overload = resolutionResult.getOverload();
            expression.setOverload(overload);

            final List<TPreptimeValue> operandValues = new ArrayList<TPreptimeValue>(operands.size());
            List<TInstance> operandInstances = new ArrayList<TInstance>(operands.size());
            boolean anyOperandsNullable = false;
            for (ExpressionNode operand : operands) {
                TPreptimeValue preptimeValue = operand.getPreptimeValue();
                operandValues.add(preptimeValue);
                operandInstances.add(preptimeValue.instance());
                if (Boolean.TRUE.equals(preptimeValue.instance().nullability()))
                    anyOperandsNullable = true;
            }

            TOverloadResult overloadResultStrategy = overload.resultStrategy();
            TInstance resultInstance;

            TPreptimeContext context = new TPreptimeContext(operandInstances, queryContext);
            switch (overloadResultStrategy.category()) {
            case CUSTOM:
                resultInstance = overloadResultStrategy.customRule().resultInstance(operandValues, context);
                break;
            case FIXED:
                resultInstance = overloadResultStrategy.fixed();
                break;
            case PICKING:
                resultInstance = resolutionResult.getPickedInstance();
                break;
            default:
                throw new AssertionError(overloadResultStrategy.category());
            }
            context.setOutputType(resultInstance);
            if (resultInstance.nullability() == null) {
                resultInstance.setNullable(anyOperandsNullable);
            }
            overload.finishPreptimePhase(context);

            // Put the preptime value, possibly including nullness, into the expression. The constant folder
            // will use it.
            TPreptimeValue preptimeValue = overload.evaluateConstant(context, new LazyListBase<TPreptimeValue>() {
                @Override
                public TPreptimeValue get(int i) {
                    return operandValues.get(i);
                }

                @Override
                public int size() {
                    return operandValues.size();
                }
            });
            if (preptimeValue == null)
                preptimeValue = new TPreptimeValue(resultInstance);
            else
                assert resultInstance.equals(preptimeValue.instance())
                        : resultInstance + " != " + preptimeValue.instance();

            expression.setPreptimeValue(preptimeValue);
            return expression;
        }

        ExpressionNode handleIfElseExpression(IfElseExpression expression) {
            ExpressionNode thenExpr = expression.getThenExpression();
            ExpressionNode elseExpr = expression.getElseExpression();

            // constant-fold if the condition is constant
            PValueSource conditionVal = pval(expression.getTestCondition());
            if (conditionVal != null) {
                boolean conditionMet = conditionVal.getBoolean(false);
                return conditionMet ? thenExpr : elseExpr;
            }

            TInstance thenType = tinst(thenExpr);
            TInstance elseType = tinst(elseExpr);

            TClass commonClass = resolver.commonTClass(thenType.typeClass(), elseType.typeClass());
            if (commonClass == null)
                throw error("couldn't determine a type for CASE expression");
            thenExpr = castTo(thenExpr, commonClass);
            elseExpr = castTo(elseExpr, commonClass);
            TInstance resultInstance = commonClass.pickInstance(tinst(thenExpr), tinst(elseExpr));
            expression.setPreptimeValue(new TPreptimeValue(resultInstance));
            return expression;
        }

        ExpressionNode handleAggregateFunctionExpression(AggregateFunctionExpression expression) {
            ExpressionNode operand = expression.getOperand();
            TInstance resultType;
            if (operand == null) {
                TAggregator tAggregator = resolver.getAggregation(expression.getFunction(), null);
                resultType = tAggregator.resultType(null);
                expression.setPreptimeValue(new TPreptimeValue(resultType));
            }
            else {
                TClass inputTClass = tclass(operand);
                TAggregator tAggregator = resolver.getAggregation(expression.getFunction(), inputTClass);
                TClass aggrTypeClass = tAggregator.getTypeClass();
                if (aggrTypeClass != null && !aggrTypeClass.equals(inputTClass)) {
                    operand = castTo(operand, aggrTypeClass);
                    expression.setOperand(operand);
                }
                resultType = tAggregator.resultType(operand.getPreptimeValue());
            }
            expression.setPreptimeValue(new TPreptimeValue(resultType));
            return expression;
        }

        ExpressionNode handleExistsCondition(ExistsCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleSubqueryValueExpression(SubqueryValueExpression expression) {
            TInstance instance = TypesTranslation.toTInstance(expression.getSQLtype());
            expression.setPreptimeValue(new TPreptimeValue(instance));
            return expression;
        }

        ExpressionNode handleSubqueryResultSetExpression(SubqueryResultSetExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleAnyCondition(AnyCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleComparisonCondition(ComparisonCondition expression) {
            ExpressionNode left = expression.getLeft();
            ExpressionNode right = expression.getRight();
            TClass leftTClass = tclass(left);
            TClass rightTClass = tclass(right);
            if (leftTClass != rightTClass) {
                TClass common = resolver.commonTClass(leftTClass, rightTClass);
                if (common == null)
                    throw error("no common type found for comparison of " + expression);
                left = castTo(left, common);
                right = castTo(right, common);
                expression.setLeft(left);
                expression.setRight(right);
            }

            return boolExpr(expression);
        }

        ExpressionNode handleColumnExpression(ColumnExpression expression) {
            Column column = expression.getColumn();
            ColumnSource columnSource = expression.getTable();
            if (column != null) {
                assert columnSource instanceof TableSource : columnSource;
                expression.setPreptimeValue(new TPreptimeValue(column.tInstance()));
            }
            else if (columnSource instanceof AggregateSource) {
                AggregateSource aggTable = (AggregateSource) columnSource;
                TPreptimeValue ptv = aggTable.getField(expression.getPosition()).getPreptimeValue();
                expression.setPreptimeValue(ptv);
            }
            else if (columnSource instanceof SubquerySource) {
                TInstance tInstance = TypesTranslation.toTInstance(expression.getSQLtype());
                expression.setPreptimeValue(new TPreptimeValue(tInstance));
                return expression;
            }
            else if (columnSource instanceof NullSource) {
                expression.setPreptimeValue(new TPreptimeValue(null));
                return expression;
            }
            else if (columnSource instanceof Project) {
                Project pTable = (Project) columnSource;
                TPreptimeValue ptv = pTable.getFields().get(expression.getPosition()).getPreptimeValue();
                expression.setPreptimeValue(ptv);
            }
            else if (columnSource instanceof ExpressionsSource) {
                ExpressionsSource exprsTable = (ExpressionsSource) columnSource;
                TPreptimeValue ptv = exprsTable.getPreptimeValues().get(expression.getPosition());
                expression.setPreptimeValue(ptv);
            }
            else {
                throw new AssertionError(columnSource + "(" + columnSource.getClass() + ")");
            }
            return expression;
        }

        ExpressionNode handleInListCondition(InListCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleParameterCondition(ParameterCondition expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleParameterExpression(ParameterExpression expression) {
            TInstance tinst = TypesTranslation.toTInstance(expression.getSQLtype());
            expression.setPreptimeValue(new TPreptimeValue(tinst));
            return expression;
        }

        ExpressionNode handleBooleanOperationExpression(BooleanOperationExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleBooleanConstantExpression(BooleanConstantExpression expression) {
            return boolExpr(expression);
        }

        ExpressionNode handleConstantExpression(ConstantExpression expression) {
            // will be lazily loaded as necessary
            return expression;
        }

        private static PValueSource pval(ExpressionNode expression) {
            return expression.getPreptimeValue().value();
        }

        private static RuntimeException error(String message) {
            throw new RuntimeException(message); // TODO what actual error type?
        }

    }

    private static ExpressionNode boolExpr(ExpressionNode expression) {
        expression.setPreptimeValue(new TPreptimeValue(AkBool.INSTANCE.instance()));
        return expression;
    }

    static class TopLevelCastingVistor implements PlanVisitor {

        private List<? extends ColumnContainer> targetColumns;

        public void apply(PlanNode plan) {
            plan.accept(this);
        }

        // PlanVisitor

        @Override
        public boolean visitEnter(PlanNode n) {
            boolean recurse = false;
            // set up the targets
            if (n instanceof InsertStatement) {
                InsertStatement insert = (InsertStatement) n;
                setTargets(insert.getTargetColumns());
                recurse = true;
            }
            else if (n instanceof UpdateStatement) {
                UpdateStatement update = (UpdateStatement) n;
                setTargets(update.getUpdateColumns());
                for (UpdateColumn updateColumn : update.getUpdateColumns()) {
                    Column target = updateColumn.getColumn();
                    ExpressionNode value = updateColumn.getExpression();
                    ExpressionNode casted = castTo(value, target.tInstance().typeClass());
                    if (casted != value)
                        updateColumn.setExpression(casted);
                }
                recurse = true;
            }

            // use the targets
            if (n instanceof Project)
                handleProject((Project) n);
            else if (n instanceof ExpressionsSource)
                handleExpressionSource((ExpressionsSource) n);
            return recurse;
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        private void handleExpressionSource(ExpressionsSource source) {
            for (List<ExpressionNode> row : source.getExpressions()) {
                castToTarget(row);
            }
        }

        private void castToTarget(List<ExpressionNode> row) {
            for (int i = 0, ncols = row.size(); i < ncols; ++i) {
                Column target = targetColumns.get(i).getColumn();
                ExpressionNode column = row.get(i);
                ExpressionNode casted = castTo(column, target.tInstance().typeClass());
                if (column != casted)
                    row.set(i, casted);
            }
        }

        private void handleProject(Project source) {
            castToTarget(source.getFields());
        }

        @Override
        public boolean visit(PlanNode n) {
            return true;
        }

        private void setTargets(List<? extends ColumnContainer> targetColumns) {
            assert this.targetColumns == null : this.targetColumns;
            this.targetColumns = targetColumns;
        }
    }

    private static ExpressionNode castTo(ExpressionNode expression, TClass targetClass) {
        // parameters and literal nulls have no type, so just set the type -- they'll be polymorphic about it.
        if (expression instanceof ParameterExpression) {
            expression.setPreptimeValue(new TPreptimeValue(targetClass.instance()));
            return expression;
        }
        if (expression instanceof NullSource) {
            PValueSource nullSource = PValueSources.getNullSource(targetClass.underlyingType());
            expression.setPreptimeValue(new TPreptimeValue(targetClass.instance(), nullSource));
            return expression;
        }

        if (targetClass.equals(tclass(expression)))
            return expression;
        TInstance instance = targetClass.instance();
        instance.setNullable(expression.getSQLtype().isNullable());
        CastExpression result
                = new CastExpression(expression, instance.dataTypeDescriptor(), expression.getSQLsource());
        result.setPreptimeValue(new TPreptimeValue(instance));
        return result;
    }

    private static TClass tclass(ExpressionNode operand) {
        TInstance tinst = tinst(operand);
        return tinst == null ? null : tinst.typeClass();
    }

    private static TInstance tinst(ExpressionNode node) {
        TPreptimeValue ptv = node.getPreptimeValue();
        return ptv == null ? null : ptv.instance();
    }
}
