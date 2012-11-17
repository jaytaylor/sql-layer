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

import com.akiban.ais.model.Routine;
import com.akiban.qp.operator.QueryContext;
import com.akiban.server.collation.AkCollator;
import com.akiban.qp.operator.API;
import com.akiban.qp.operator.Operator;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchCastException;
import com.akiban.server.expression.std.Comparison;
import com.akiban.server.expression.std.ExpressionTypes;
import com.akiban.server.t3expressions.OverloadResolver;
import com.akiban.server.t3expressions.OverloadResolver.OverloadResult;
import com.akiban.server.t3expressions.T3RegistryService;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TComparison;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.aksql.akfuncs.AkIfElse;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.pvalue.PUnderlying;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.texpressions.TCastExpression;
import com.akiban.server.types3.texpressions.TComparisonExpression;
import com.akiban.server.types3.texpressions.TComparisonExpressionBase;
import com.akiban.server.types3.texpressions.TInExpression;
import com.akiban.server.types3.texpressions.TNullExpression;
import com.akiban.server.types3.texpressions.TPreparedBoundField;
import com.akiban.server.types3.texpressions.TPreparedExpression;
import com.akiban.server.types3.texpressions.TPreparedField;
import com.akiban.server.types3.texpressions.TPreparedFunction;
import com.akiban.server.types3.texpressions.TPreparedLiteral;
import com.akiban.server.types3.texpressions.TPreparedParameter;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.server.types3.texpressions.TValidatedScalar;
import com.akiban.sql.optimizer.plan.AggregateSource;
import com.akiban.sql.optimizer.plan.BooleanConstantExpression;
import com.akiban.sql.optimizer.plan.BooleanOperationExpression;
import com.akiban.sql.optimizer.plan.CastExpression;
import com.akiban.sql.optimizer.plan.ComparisonCondition;
import com.akiban.sql.optimizer.plan.ConditionExpression;
import com.akiban.sql.optimizer.plan.ConstantExpression;
import com.akiban.sql.optimizer.plan.ExpressionNode;
import com.akiban.sql.optimizer.plan.FunctionExpression;
import com.akiban.sql.optimizer.plan.IfElseExpression;
import com.akiban.sql.optimizer.plan.ParameterExpression;
import com.akiban.sql.optimizer.plan.ResolvableExpression;
import com.akiban.sql.script.ScriptBindingsRoutineTExpression;
import com.akiban.sql.script.ScriptFunctionJavaRoutineTExpression;
import com.akiban.sql.server.ServerJavaMethodTExpression;
import com.akiban.sql.types.CharacterTypeAttributes;
import com.akiban.util.SparseArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public final class NewExpressionAssembler extends ExpressionAssembler<TPreparedExpression> {
    private static final Logger logger = LoggerFactory.getLogger(NewExpressionAssembler.class);

    private static final TValidatedScalar ifElseValidated = new TValidatedScalar(AkIfElse.INSTANCE);

    private final T3RegistryService registryService;
    private final QueryContext queryContext;

    public NewExpressionAssembler(PlanContext planContext) {
        super(planContext);
        RulesContext rulesContext = planContext.getRulesContext();
        registryService = ((SchemaRulesContext)rulesContext).getT3Registry();
        queryContext = planContext.getQueryContext();
    }

    @Override
    protected TPreparedExpression assembleFunction(ExpressionNode functionNode, String functionName,
                                                   List<ExpressionNode> argumentNodes,
                                                   ColumnExpressionContext columnContext,
                                                   SubqueryOperatorAssembler<TPreparedExpression> subqueryAssembler) {

        List<TPreparedExpression> arguments = assembleExpressions(argumentNodes, columnContext, subqueryAssembler);
        TValidatedScalar overload;
        SparseArray<Object> preptimeValues = null;
        if (functionNode instanceof FunctionExpression) {
            FunctionExpression fexpr = (FunctionExpression) functionNode;
            overload = fexpr.getResolved();
            preptimeValues = fexpr.getPreptimeValues();
        }
        else if (functionNode instanceof BooleanOperationExpression) {
            List<TPreptimeValue> inputPreptimeValues = new ArrayList<TPreptimeValue>(argumentNodes.size());
            for (ExpressionNode argument : argumentNodes) {
                inputPreptimeValues.add(argument.getPreptimeValue());
            }

            OverloadResolver<TValidatedScalar> scalarsResolver = registryService.getScalarsResolver();
            OverloadResult<TValidatedScalar> overloadResult = scalarsResolver.get(functionName, inputPreptimeValues);
            overload = overloadResult.getOverload();
        }
        else if (functionNode instanceof IfElseExpression) {
            overload = ifElseValidated;
        }
        else {
            throw new AssertionError(functionNode);
        }
         TInstance resultInstance = functionNode.getPreptimeValue().instance();
         return new TPreparedFunction(overload, resultInstance, arguments, queryContext, preptimeValues);
    }

    @Override
    protected TPreparedExpression assembleCastExpression(CastExpression castExpression,
                                                         ColumnExpressionContext columnContext,
                                                         SubqueryOperatorAssembler<TPreparedExpression> subqueryAssembler) {
        TInstance toType = castExpression.getPreptimeValue().instance();
        TPreparedExpression expr = assembleExpression(castExpression.getOperand(), columnContext, subqueryAssembler);
        if (toType == null)
            return expr;
        TInstance sourceInstance = expr.resultType();
        if (sourceInstance == null) // CAST(NULL as FOOTYPE)
        {
            toType = toType.withNullable(true);
            return new TNullExpression(toType);
        }
        else if (!toType.equals(sourceInstance))
        {
            // Do type conversion.
            TCast tcast = registryService.getCastsResolver().cast(sourceInstance, toType);
            if (tcast == null) {
                throw new NoSuchCastException(sourceInstance, toType);
            }
            expr = new TCastExpression(expr, tcast, toType, queryContext);
        }
        return expr;
    }

    @Override
    protected TPreparedExpression tryLiteral(ExpressionNode node) {
        TPreparedExpression result = null;
        TPreptimeValue tpv = node.getPreptimeValue();
        if (tpv != null) {
            TInstance instance = tpv.instance();
            PValueSource value = tpv.value();
            if (instance != null && value != null)
                result = new TPreparedLiteral(instance, value);
        }
        return result;
    }

    @Override
    protected TPreparedExpression literal(ConstantExpression expression) {
        TPreptimeValue ptval = expression.getPreptimeValue();
        return new TPreparedLiteral(ptval.instance(), ptval.value());
    }

    @Override
    protected TPreparedExpression variable(ParameterExpression expression) {
        return new TPreparedParameter(expression.getPosition(), expression.getPreptimeValue().instance());
    }

    @Override
    protected TPreparedExpression compare(TPreparedExpression left, ComparisonCondition comparison, TPreparedExpression right) {
        TKeyComparable keyComparable = comparison.getKeyComparable();
        if (keyComparable != null) {
            return new TKeyComparisonPreparation(left, comparison.getOperation(), right, comparison.getKeyComparable());
        }
        else {
            return new TComparisonExpression(left, comparison.getOperation(), right);
        }
    }

    @Override
    protected TPreparedExpression collate(TPreparedExpression left, Comparison comparison, TPreparedExpression right, AkCollator collator) {
        return new TComparisonExpression(left,  comparison, right, collator);
    }

    @Override
    protected AkCollator collator(ComparisonCondition cond, TPreparedExpression left, TPreparedExpression right) {
        TInstance leftInstance = left.resultType();
        TInstance rightInstance = right.resultType();
        TClass tClass = leftInstance.typeClass();
        assert tClass.compatibleForCompare(rightInstance.typeClass())
                : tClass + " != " + rightInstance.typeClass();
        if (tClass.underlyingType() != PUnderlying.STRING)
            return null;
        CharacterTypeAttributes leftAttributes = StringAttribute.characterTypeAttributes(leftInstance);
        CharacterTypeAttributes rightAttributes = StringAttribute.characterTypeAttributes(rightInstance);
        return ExpressionTypes.mergeAkCollators(leftAttributes, rightAttributes);
    }

    @Override
    protected TPreparedExpression in(TPreparedExpression lhs, List<TPreparedExpression> rhs) {
        return TInExpression.prepare(lhs, rhs, queryContext);
    }

    @Override
    protected TPreparedExpression assembleFieldExpression(RowType rowType, int fieldIndex) {
        return new TPreparedField(rowType.typeInstanceAt(fieldIndex), fieldIndex);
    }

    @Override
    protected TPreparedExpression assembleBoundFieldExpression(RowType rowType, int rowIndex, int fieldIndex) {
        return new TPreparedBoundField(rowType, rowIndex, fieldIndex);
    }

    @Override
    protected TPreparedExpression assembleRoutine(ExpressionNode routineNode, 
                                                  Routine routine,
                                                  List<ExpressionNode> argumentNodes,
                                                  ColumnExpressionContext columnContext,
                                                  SubqueryOperatorAssembler<TPreparedExpression> subqueryAssembler) {
        List<TPreparedExpression> operands = assembleExpressions(argumentNodes, columnContext, subqueryAssembler);
        switch (routine.getCallingConvention()) {
        case JAVA:
            return new ServerJavaMethodTExpression(routine, operands);
        case SCRIPT_FUNCTION_JAVA:
            return new ScriptFunctionJavaRoutineTExpression(routine, operands);
        case SCRIPT_BINDINGS:
            return new ScriptBindingsRoutineTExpression(routine, operands);
        default:
            throw new AkibanInternalException("Unimplemented routine " + routine);
        }
    }

    @Override
    public Operator assembleAggregates(Operator inputOperator, RowType rowType, int nkeys, AggregateSource aggregateSource) {
        List<ResolvableExpression<TValidatedAggregator>> aggregates = aggregateSource.getResolved();
        int naggrs = aggregates.size();
        List<TAggregator> aggregators = new ArrayList<TAggregator>(naggrs);
        List<TInstance> outputInstances = new ArrayList<TInstance>(naggrs);
        for (int i = 0; i < naggrs; ++i) {
            ResolvableExpression<TValidatedAggregator> aggr = aggregates.get(i);
            aggregators.add(aggr.getResolved());
            outputInstances.add(aggr.getPreptimeValue().instance());
        }
        return API.aggregate_Partial(
                inputOperator,
                rowType,
                nkeys,
                aggregators,
                outputInstances,
                aggregateSource.getOptions());
    }

    @Override
    public ConstantExpression evalNow(PlanContext planContext, ExpressionNode node) {
        if (node instanceof ConstantExpression)
            return (ConstantExpression)node;
        TPreparedExpression expr = assembleExpression(node, null, null);
        TPreptimeValue preptimeValue = expr.evaluateConstant(planContext.getQueryContext());
        if (preptimeValue == null)
            throw new AkibanInternalException("required constant expression: " + expr);
        PValueSource valueSource = preptimeValue.value();
        if (valueSource == null)
            throw new AkibanInternalException("required constant expression: " + expr);
        if (node instanceof ConditionExpression) {
            Boolean value = valueSource.isNull() ? null : valueSource.getBoolean();
            return new BooleanConstantExpression(value,
                    node.getSQLtype(),
                    node.getSQLsource());
        }
        else {
            return new ConstantExpression(preptimeValue);
        }
    }

    @Override
    protected Logger logger() {
        return logger;
    }

    private class TKeyComparisonPreparation extends TComparisonExpressionBase {

        private TKeyComparisonPreparation(TPreparedExpression left, Comparison op, TPreparedExpression right,
                                          TKeyComparable comparable)
        {
            super(left, op, right);
            this.comparison = comparable.getComparison();
            TClass leftIn = tClass(left);
            TClass rightIn = tClass(right);
            TClass leftCmp = comparable.getLeftTClass();
            TClass rightCmp = comparable.getRightTClass();
            if (leftIn == leftCmp && rightIn == rightCmp) {
                reverseComparison = false;
            }
            else if (rightIn == leftCmp && leftIn == rightCmp) {
                reverseComparison = true;
            }
            else {
                throw new IllegalArgumentException(
                        "invalid comparisons: " + left + " and " + right + " against " + comparable);
            }
        }

        private TClass tClass(TPreparedExpression left) {
            TInstance tInstance = left.resultType();
            return tInstance == null ? null : tInstance.typeClass();
        }

        @Override
        protected int compare(TInstance leftInstance, PValueSource left, TInstance rightInstance,
                                  PValueSource right) {
            return reverseComparison
                    ? - comparison.compare(rightInstance, right, leftInstance, left)
                    :   comparison.compare(leftInstance, left, rightInstance, right);
        }

        private final TComparison comparison;
        private final boolean reverseComparison;
    }
}
