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

import com.foundationdb.sql.optimizer.plan.AggregateFunctionExpression;
import com.foundationdb.sql.optimizer.plan.AggregateSource;
import com.foundationdb.sql.optimizer.plan.BooleanConstantExpression;
import com.foundationdb.sql.optimizer.plan.BooleanOperationExpression;
import com.foundationdb.sql.optimizer.plan.CastExpression;
import com.foundationdb.sql.optimizer.plan.ColumnExpression;
import com.foundationdb.sql.optimizer.plan.ColumnDefaultExpression;
import com.foundationdb.sql.optimizer.plan.ComparisonCondition;
import com.foundationdb.sql.optimizer.plan.ConditionExpression;
import com.foundationdb.sql.optimizer.plan.ConstantExpression;
import com.foundationdb.sql.optimizer.plan.ExpressionNode;
import com.foundationdb.sql.optimizer.plan.FunctionExpression;
import com.foundationdb.sql.optimizer.plan.IfElseExpression;
import com.foundationdb.sql.optimizer.plan.InListCondition;
import com.foundationdb.sql.optimizer.plan.IsNullIndexKey;
import com.foundationdb.sql.optimizer.plan.ParameterExpression;
import com.foundationdb.sql.optimizer.plan.ResolvableExpression;
import com.foundationdb.sql.optimizer.plan.RoutineExpression;
import com.foundationdb.sql.optimizer.plan.SubqueryExpression;
import com.foundationdb.sql.optimizer.plan.CreateAs;
import com.foundationdb.sql.optimizer.plan.TableSource;
import com.foundationdb.sql.script.ScriptBindingsRoutineTExpression;
import com.foundationdb.sql.script.ScriptFunctionJavaRoutineTExpression;
import com.foundationdb.sql.server.ServerJavaMethodTExpression;
import com.foundationdb.sql.types.CharacterTypeAttributes;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.TableName;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.operator.QueryContext;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.NoSuchCastException;
import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.server.explain.CompoundExplainer;
import com.foundationdb.server.explain.Label;
import com.foundationdb.server.explain.PrimitiveExplainer;
import com.foundationdb.server.explain.Type;
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
import com.foundationdb.server.types.service.OverloadResolver;
import com.foundationdb.server.types.service.OverloadResolver.OverloadResult;
import com.foundationdb.server.types.service.TypesRegistryService;
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
import com.foundationdb.server.types.value.UnderlyingType;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.util.SparseArray;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class ExpressionAssembler 
{
    private static final Logger logger = LoggerFactory.getLogger(ExpressionAssembler.class);

    private static final TValidatedScalar ifElseValidated = new TValidatedScalar(AkIfElse.INSTANCE);

    private final PlanContext planContext;
    private final PlanExplainContext explainContext;
    private final SchemaRulesContext rulesContext;
    private final TypesRegistryService registryService;
    private final QueryContext queryContext;
    private static  int CREATE_AS_BINDING_POSITION = 2;

    public ExpressionAssembler(PlanContext planContext) {
        this.planContext = planContext;
        if (planContext instanceof ExplainPlanContext)
            explainContext = ((ExplainPlanContext)planContext).getExplainContext();
        else
            explainContext = null;
        rulesContext = (SchemaRulesContext)planContext.getRulesContext();
        registryService = rulesContext.getTypesRegistry();
        queryContext = planContext.getQueryContext();
    }

    public TPreparedExpression assembleExpression(ExpressionNode node,
                                                  ColumnExpressionContext columnContext,
                                                  SubqueryOperatorAssembler subqueryAssembler)  {
        TPreparedExpression possiblyLiteral = tryLiteral(node);
        if (possiblyLiteral != null)
            return possiblyLiteral;
        if (node instanceof ConstantExpression)
            return literal(((ConstantExpression)node));
        else if (node instanceof ColumnExpression)
            return assembleColumnExpression((ColumnExpression)node, columnContext);
        else if (node instanceof ParameterExpression)
            return variable((ParameterExpression)node);
        else if (node instanceof BooleanOperationExpression) {
            BooleanOperationExpression bexpr = (BooleanOperationExpression)node;
            return assembleFunction(bexpr, bexpr.getOperation().getFunctionName(),
                    Arrays.<ExpressionNode>asList(bexpr.getLeft(), bexpr.getRight()),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof CastExpression)
            return assembleCastExpression((CastExpression)node,
                    columnContext, subqueryAssembler);
        else if (node instanceof ComparisonCondition) {
            ComparisonCondition cond = (ComparisonCondition)node;
            TPreparedExpression left = assembleExpression(cond.getLeft(), columnContext, subqueryAssembler);
            TPreparedExpression right = assembleExpression(cond.getRight(), columnContext, subqueryAssembler);
            // never use a collator if we have a KeyComparable
            AkCollator collator = (cond.getKeyComparable() == null) ? collator(cond, left, right) : null;
            if (collator != null)
                return collate(left, cond.getOperation(), right, collator);
            else
                return compare(left, cond, right);
        }
        else if (node instanceof FunctionExpression) {
            FunctionExpression funcNode = (FunctionExpression)node;
            return assembleFunction(funcNode, funcNode.getFunction(),
                    funcNode.getOperands(),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof IfElseExpression) {
            IfElseExpression ifElse = (IfElseExpression)node;
            return assembleFunction(ifElse, "if",
                    Arrays.asList(ifElse.getTestCondition(),
                            ifElse.getThenExpression(),
                            ifElse.getElseExpression()),
                    columnContext, subqueryAssembler);
        }
        else if (node instanceof InListCondition) {
            InListCondition inList = (InListCondition)node;
            TPreparedExpression lhs = assembleExpression(inList.getOperand(),
                    columnContext, subqueryAssembler);
            List<TPreparedExpression> rhs = assembleExpressions(inList.getExpressions(),
                    columnContext, subqueryAssembler);
            return in(lhs, rhs, inList);
        }
        else if (node instanceof RoutineExpression) {
            RoutineExpression routineNode = (RoutineExpression)node;
            return assembleRoutine(routineNode, routineNode.getRoutine(),
                                   routineNode.getOperands(),
                                   columnContext, subqueryAssembler);
        }
        else if (node instanceof SubqueryExpression)
            return subqueryAssembler.assembleSubqueryExpression((SubqueryExpression)node);
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function",
                    node.getSQLsource());
        else if (node instanceof ColumnDefaultExpression)
            return assembleColumnDefault(((ColumnDefaultExpression)node).getColumn(), null);
        else if (node instanceof IsNullIndexKey)
            return new TNullExpression(node.getType());
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
    }

    private List<TPreparedExpression> assembleExpressions(List<ExpressionNode> expressions,
                                                          ColumnExpressionContext columnContext,
                                                          SubqueryOperatorAssembler subqueryAssembler) {
        List<TPreparedExpression> result = new ArrayList<>(expressions.size());
        for (ExpressionNode expr : expressions) {
            result.add(assembleExpression(expr, columnContext, subqueryAssembler));
        }
        return result;
    }

    private TPreparedExpression assembleColumnExpression(ColumnExpression column,
                                                         ColumnExpressionContext columnContext) {
        if (column.getTable() instanceof CreateAs) {
            RowType rowType = columnContext.getRowType((CreateAs)column.getTable());
            TPreparedExpression expression = assembleBoundFieldExpression(rowType, CREATE_AS_BINDING_POSITION, column.getPosition());
            if (explainContext != null)
                explainColumnExpression(expression, column);
            return expression;
        }
        ColumnExpressionToIndex currentRow = columnContext.getCurrentRow();
        if (currentRow != null) {
            int fieldIndex = currentRow.getIndex(column);
            if (fieldIndex >= 0) {
                TPreparedExpression expression = assembleFieldExpression(currentRow.getRowType(), fieldIndex);
                if (explainContext != null)
                    explainColumnExpression(expression, column);
                return expression;
            }
        }

        for (ColumnExpressionToIndex boundRow : columnContext.getBoundRows()) {
            int fieldIndex = boundRow.getIndex(column);
            if (fieldIndex >= 0) {
                int rowIndex = columnContext.getBindingPosition(boundRow);
                TPreparedExpression expression = assembleBoundFieldExpression(boundRow.getRowType(), rowIndex, fieldIndex);
                if (explainContext != null)
                    explainColumnExpression(expression, column);
                return expression;
            }
        }
        if(column.getTable() instanceof TableSource){

            RowType rowType = columnContext.getRowType(column.getColumn().getTable().getTableId());
            TPreparedExpression expression = assembleBoundFieldExpression(rowType, CREATE_AS_BINDING_POSITION, column.getPosition());
            if (explainContext != null)
                explainColumnExpression(expression, column);
            return expression;
        }

        logger.debug("Did not find {} from {} in {}",
                     new Object[] { 
                         column, column.getTable(), columnContext.getBoundRows() 
                     });
        throw new AkibanInternalException("Column not found " + column);
    }

    private TPreparedExpression assembleFunction(ExpressionNode functionNode, String functionName,
                                                 List<ExpressionNode> argumentNodes,
                                                 ColumnExpressionContext columnContext,
                                                 SubqueryOperatorAssembler subqueryAssembler) {

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
         TInstance resultInstance = functionNode.getType();
         return new TPreparedFunction(overload, resultInstance, arguments, preptimeValues);
    }

    private TPreparedExpression assembleCastExpression(CastExpression castExpression,
                                                       ColumnExpressionContext columnContext,
                                                       SubqueryOperatorAssembler subqueryAssembler) {
        TInstance toType = castExpression.getType();
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
            expr = new TCastExpression(expr, tcast, toType);
        }
        return expr;
    }

    private TPreparedExpression tryLiteral(ExpressionNode node) {
        TPreparedExpression result = null;
        TPreptimeValue tpv = node.getPreptimeValue();
        if (tpv != null) {
            TInstance type = tpv.type();
            ValueSource value = tpv.value();
            if (type != null && value != null)
                result = new TPreparedLiteral(type, value);
        }
        return result;
    }

    private TPreparedExpression literal(ConstantExpression expression) {
        TPreptimeValue ptval = expression.getPreptimeValue();
        return new TPreparedLiteral(ptval.type(), ptval.value());
    }

    private TPreparedExpression variable(ParameterExpression expression) {
        return new TPreparedParameter(expression.getPosition(), expression.getType());
    }

    private TPreparedExpression compare(TPreparedExpression left, ComparisonCondition comparison, TPreparedExpression right) {
        TKeyComparable keyComparable = comparison.getKeyComparable();
        if (keyComparable != null) {
            return new TKeyComparisonPreparation(left, comparison.getOperation(), right, comparison.getKeyComparable());
        }
        else {
            return new TComparisonExpression(left, comparison.getOperation(), right);
        }
    }

    private TPreparedExpression collate(TPreparedExpression left, Comparison comparison, TPreparedExpression right, AkCollator collator) {
        return new TComparisonExpression(left,  comparison, right, collator);
    }

    private AkCollator collator(ComparisonCondition cond, TPreparedExpression left, TPreparedExpression right) {
        TInstance leftInstance = left.resultType();
        TInstance rightInstance = right.resultType();
        TClass tClass = leftInstance.typeClass();
        assert tClass.compatibleForCompare(rightInstance.typeClass())
                : tClass + " != " + rightInstance.typeClass();
        if (tClass.underlyingType() != UnderlyingType.STRING)
            return null;
        CharacterTypeAttributes leftAttributes = StringAttribute.characterTypeAttributes(leftInstance);
        CharacterTypeAttributes rightAttributes = StringAttribute.characterTypeAttributes(rightInstance);
        return TString.mergeAkCollators(leftAttributes, rightAttributes);
    }

    private TPreparedExpression in(TPreparedExpression lhs, List<TPreparedExpression> rhs, InListCondition inList) {
        ComparisonCondition comparison = inList.getComparison();
        if (comparison == null)
            return TInExpression.prepare(lhs, rhs, queryContext);
        else
            return TInExpression.prepare(lhs, rhs, 
                                         comparison.getRight().getType(),
                                         comparison.getKeyComparable(), 
                                         queryContext);
    }

    private TPreparedExpression assembleFieldExpression(RowType rowType, int fieldIndex) {
        return new TPreparedField(rowType.typeAt(fieldIndex), fieldIndex);
    }

    private TPreparedExpression assembleBoundFieldExpression(RowType rowType, int rowIndex, int fieldIndex) {
        return new TPreparedBoundField(rowType, rowIndex, fieldIndex);
    }

    private TPreparedExpression assembleRoutine(ExpressionNode routineNode, 
                                                Routine routine,
                                                List<ExpressionNode> operandNodes,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler subqueryAssembler) {
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
            throw new AkibanInternalException("Unimplemented routine " + routine.getName());
        }
    }

    public Operator assembleAggregates(Operator inputOperator, RowType rowType, int nkeys, AggregateSource aggregateSource) {
        List<ResolvableExpression<TValidatedAggregator>> aggregates = aggregateSource.getResolved();
        int naggrs = aggregates.size();
        List<TAggregator> aggregators = new ArrayList<>(naggrs);
        List<TInstance> outputInstances = new ArrayList<>(naggrs);
        for (int i = 0; i < naggrs; ++i) {
            ResolvableExpression<TValidatedAggregator> aggr = aggregates.get(i);
            aggregators.add(aggr.getResolved());
            outputInstances.add(aggr.getType());
        }
        return API.aggregate_Partial(
                inputOperator,
                rowType,
                nkeys,
                aggregators,
                outputInstances,
                aggregateSource.getOptions());
    }

    // Changes here probably need reflected in OnlineHelper#buildColumnDefault()
    public TPreparedExpression assembleColumnDefault(Column column, TPreparedExpression expression) {
        return PlanGenerator.generateDefaultExpression(column,
                                                       expression,
                                                       registryService,
                                                       rulesContext.getTypesTranslator(),
                                                       planContext.getQueryContext());
    }

    public ConstantExpression evalNow(PlanContext planContext, ExpressionNode node) {
        if (node instanceof ConstantExpression)
            return (ConstantExpression)node;
        TPreparedExpression expr = assembleExpression(node, null, null);
        TPreptimeValue preptimeValue = expr.evaluateConstant(planContext.getQueryContext());
        if (preptimeValue == null)
            throw new AkibanInternalException("required constant expression: " + expr);
        ValueSource valueSource = preptimeValue.value();
        if (valueSource == null)
            throw new AkibanInternalException("required constant expression: " + expr);
        if (node instanceof ConditionExpression) {
            Boolean value = valueSource.isNull() ? null : valueSource.getBoolean();
            return new BooleanConstantExpression(value);
        }
        else {
            return new ConstantExpression(preptimeValue);
        }
    }

    private static class TKeyComparisonPreparation extends TComparisonExpressionBase {

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
            TInstance type = left.resultType();
            return type == null ? null : type.typeClass();
        }

        @Override
        protected int compare(TInstance leftInstance, ValueSource left, TInstance rightInstance,
                                  ValueSource right) {
            return reverseComparison
                    ? - comparison.compare(rightInstance, right, leftInstance, left)
                    :   comparison.compare(leftInstance, left, rightInstance, right);
        }

        private final TComparison comparison;
        private final boolean reverseComparison;
    }

    private void explainColumnExpression(TPreparedExpression expression, ColumnExpression column) {
        CompoundExplainer explainer = new CompoundExplainer(Type.EXTRA_INFO);
        explainer.addAttribute(Label.POSITION, 
                               PrimitiveExplainer.getInstance(column.getPosition()));
        Column aisColumn = column.getColumn();
        if (aisColumn != null) {
            explainer.addAttribute(Label.TABLE_CORRELATION, 
                                   PrimitiveExplainer.getInstance(column.getTable().getName()));
            TableName tableName = aisColumn.getTable().getName();
            explainer.addAttribute(Label.TABLE_SCHEMA,
                                   PrimitiveExplainer.getInstance(tableName.getSchemaName()));
            explainer.addAttribute(Label.TABLE_NAME,
                                   PrimitiveExplainer.getInstance(tableName.getTableName()));
            explainer.addAttribute(Label.COLUMN_NAME,
                                   PrimitiveExplainer.getInstance(aisColumn.getName()));
        }
        explainContext.putExtraInfo(expression, explainer);
    }

    public void setCreateAsBindingPosition(int position){
        CREATE_AS_BINDING_POSITION = position;
    }

    public interface ColumnExpressionToIndex {
        /** Return the field position of the given column in the target row. */
        public int getIndex(ColumnExpression column);

        /** Return the row type that the index goes with. */
        public RowType getRowType();
    }

    public interface ColumnExpressionContext {
        /** Get the current input row if any. */
        public ColumnExpressionToIndex getCurrentRow();

        /** Get list (deepest first) of rows from nested loops. */
        public Iterable<ColumnExpressionToIndex> getBoundRows();

        /** Get the position associated with the given row.
         */
        public int getBindingPosition(ColumnExpressionToIndex boundRow);

        public RowType getRowType(CreateAs createAs);

        public RowType getRowType(int tableID);
    }

    public interface SubqueryOperatorAssembler {
        /** Assemble the given subquery expression. */
        public TPreparedExpression assembleSubqueryExpression(SubqueryExpression subqueryExpression);
    }
}
