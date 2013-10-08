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

import com.foundationdb.ais.model.Routine;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.NoSuchCastException;
import com.foundationdb.server.t3expressions.OverloadResolver;
import com.foundationdb.server.t3expressions.OverloadResolver.OverloadResult;
import com.foundationdb.server.t3expressions.T3RegistryService;
import com.foundationdb.server.types.TAggregator;
import com.foundationdb.server.types.TCast;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TComparison;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.aksql.akfuncs.AkIfElse;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.pvalue.PUnderlying;
import com.foundationdb.server.types.pvalue.PValueSource;
import com.foundationdb.server.types.texpressions.Comparison;
import com.foundationdb.server.types.texpressions.TCastExpression;
import com.foundationdb.server.types.texpressions.TComparisonExpression;
import com.foundationdb.server.types.texpressions.TComparisonExpressionBase;
import com.foundationdb.server.types.texpressions.TInExpression;
import com.foundationdb.server.types.texpressions.TNullExpression;
import com.foundationdb.server.types.texpressions.TPreparedBoundField;
import com.foundationdb.server.types.texpressions.TPreparedExpression;
import com.foundationdb.server.types.texpressions.TPreparedField;
import com.foundationdb.server.types.texpressions.TPreparedFunction;
import com.foundationdb.server.types.texpressions.TPreparedLiteral;
import com.foundationdb.server.types.texpressions.TPreparedParameter;
import com.foundationdb.server.types.texpressions.TValidatedAggregator;
import com.foundationdb.server.types.texpressions.TValidatedScalar;
import com.foundationdb.sql.optimizer.plan.AggregateSource;
import com.foundationdb.sql.optimizer.plan.BooleanConstantExpression;
import com.foundationdb.sql.optimizer.plan.BooleanOperationExpression;
import com.foundationdb.sql.optimizer.plan.CastExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.FunctionExpression;
import com.foundationdb.sql.optimizer.plan.IfElseExpression;
import com.foundationdb.sql.optimizer.plan.InListCondition;
import com.foundationdb.sql.optimizer.plan.ParameterExpression;
import com.foundationdb.sql.optimizer.plan.ResolvableExpression;
import com.foundationdb.sql.script.ScriptBindingsRoutineTExpression;
import com.foundationdb.sql.script.ScriptFunctionJavaRoutineTExpression;
import com.foundationdb.sql.server.ServerJavaMethodTExpression;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.util.SparseArray;
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
            List<TPreptimeValue> inputPreptimeValues = new ArrayList<>(argumentNodes.size());
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
        return TString.mergeAkCollators(leftAttributes, rightAttributes);
    }

    @Override
    protected TPreparedExpression in(TPreparedExpression lhs, List<TPreparedExpression> rhs, InListCondition inList) {
        ComparisonCondition comparison = inList.getComparison();
        if (comparison == null)
            return TInExpression.prepare(lhs, rhs, queryContext);
        else
            return TInExpression.prepare(lhs, rhs, 
                                         comparison.getRight().getPreptimeValue().instance(), 
                                         comparison.getKeyComparable(), 
                                         queryContext);
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
                                                  List<ExpressionNode> operandNodes,
                                                  ColumnExpressionContext columnContext,
                                                  SubqueryOperatorAssembler<TPreparedExpression> subqueryAssembler) {
        List<TPreparedExpression> inputs = assembleExpressions(operandNodes, columnContext, subqueryAssembler);
        switch (routine.getCallingConvention()) {
        case JAVA:
            return new ServerJavaMethodTExpression(routine, inputs);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return new ScriptFunctionJavaRoutineTExpression(routine, inputs);
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
            return new ScriptBindingsRoutineTExpression(routine, inputs);
        default:
            throw new AkibanInternalException("Unimplemented routine " + routine);
        }
    }

    @Override
    public Operator assembleAggregates(Operator inputOperator, RowType rowType, int nkeys, AggregateSource aggregateSource) {
        List<ResolvableExpression<TValidatedAggregator>> aggregates = aggregateSource.getResolved();
        int naggrs = aggregates.size();
        List<TAggregator> aggregators = new ArrayList<>(naggrs);
        List<TInstance> outputInstances = new ArrayList<>(naggrs);
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
