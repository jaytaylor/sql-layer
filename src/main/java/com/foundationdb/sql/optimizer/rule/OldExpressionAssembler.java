/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
import com.foundationdb.server.collation.AkCollator;
import com.foundationdb.qp.operator.API;
import com.foundationdb.qp.operator.Operator;
import com.foundationdb.qp.rowtype.RowType;
import com.foundationdb.server.expression.std.Comparison;
import com.foundationdb.server.expression.std.Expressions;
import com.foundationdb.server.expression.std.InExpression;
import com.foundationdb.sql.optimizer.TypesTranslation;
import com.foundationdb.sql.optimizer.plan.*;
import com.foundationdb.sql.script.ScriptBindingsRoutineExpression;
import com.foundationdb.sql.script.ScriptFunctionJavaRoutineExpression;
import com.foundationdb.sql.server.ServerJavaMethodExpression;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionEvaluation;
import com.foundationdb.server.expression.ExpressionType;
import com.foundationdb.server.expression.std.ExpressionTypes;
import com.foundationdb.server.expression.std.IntervalCastExpression;
import static com.foundationdb.server.expression.std.Expressions.*;
import com.foundationdb.server.service.functions.FunctionsRegistry;
import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.Extractors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/** Turn {@link ExpressionNode} into {@link Expression}. */
public class OldExpressionAssembler extends ExpressionAssembler<Expression>
{
    private static final Logger logger = LoggerFactory.getLogger(OldExpressionAssembler.class);

    private final FunctionsRegistry functionsRegistry;

    public OldExpressionAssembler(PlanContext planContext) {
        super(planContext);
        RulesContext rulesContext = planContext.getRulesContext();
        functionsRegistry = ((SchemaRulesContext)rulesContext).getFunctionsRegistry();
    }

    public FunctionsRegistry getFunctionRegistry()
    {
        return functionsRegistry;
    }

    @Override
    protected Expression assembleFunction(ExpressionNode functionNode,
                                       String functionName,
                                       List<ExpressionNode> argumentNodes,
                                       ColumnExpressionContext columnContext,
                                       SubqueryOperatorAssembler<Expression> subqueryAssembler) {
        List<Expression> arguments = 
            assembleExpressions(argumentNodes, columnContext, subqueryAssembler);
        int nargs = arguments.size();
        List<ExpressionType> types = new ArrayList<>(nargs + 1);
        for (int i = 0; i < nargs; i++) {
            types.add(TypesTranslation.toExpressionType(argumentNodes.get(i).getSQLtype()));
        }
        types.add(TypesTranslation.toExpressionType(functionNode.getSQLtype()));
        return functionsRegistry.composer(functionName).compose(arguments, types);
    }

    @Override
    protected Expression assembleCastExpression(CastExpression castExpression,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler<Expression> subqueryAssembler) {
        ExpressionNode operand = castExpression.getOperand();
        Expression expr = assembleExpression(operand, columnContext, subqueryAssembler);
        AkType toType = castExpression.getAkType();
        if (toType == null) return expr;
        if (!toType.equals(operand.getAkType()))
        {
            // Do type conversion.
            TypeId id = castExpression.getSQLtype().getTypeId(); 
            if (id.isIntervalTypeId())
                expr = new IntervalCastExpression(expr, id);
            else 
                expr = new com.foundationdb.server.expression.std.CastExpression(toType, expr);
        }
        
        switch (toType) {
        case VARCHAR:
            {
                DataTypeDescriptor fromSQL = operand.getSQLtype();
                DataTypeDescriptor toSQL = castExpression.getSQLtype();
                if ((toSQL != null) &&
                    (toSQL.getMaximumWidth() > 0) &&
                    ((fromSQL == null) ||
                     (toSQL.getMaximumWidth() < fromSQL.getMaximumWidth())))
                    // Cast to shorter VARCHAR.
                    expr = new com.foundationdb.server.expression.std.TruncateStringExpression(toSQL.getMaximumWidth(), expr);
            }
            break;
        case DECIMAL:
            {
                DataTypeDescriptor fromSQL = operand.getSQLtype();
                DataTypeDescriptor toSQL = castExpression.getSQLtype();
                if ((toSQL != null) && !toSQL.equals(fromSQL))
                    // Cast to DECIMAL scale.
                    expr = new com.foundationdb.server.expression.std.ScaleDecimalExpression(toSQL.getPrecision(), toSQL.getScale(), expr);
            }
            break;
        }
        return expr;
    }

    @Override
    public ConstantExpression evalNow(PlanContext planContext, ExpressionNode node) {
        if (node instanceof ConstantExpression)
            return (ConstantExpression)node;
        Expression expr = assembleExpression(node, null, null);
        if (!expr.isConstant())
            throw new AkibanInternalException("required constant expression: " + expr);
        ExpressionEvaluation eval = expr.evaluation();
        eval.of(planContext.getQueryContext());
        ValueSource valueSource = eval.eval();
        if (node instanceof ConditionExpression) {
            Boolean value = Extractors.getBooleanExtractor().getBoolean(valueSource, null);
            return new BooleanConstantExpression(value,
                                                 node.getSQLtype(), 
                                                 node.getSQLsource());
        }
        else {
            return new ConstantExpression(valueSource,
                                          node.getSQLtype(), 
                                          node.getSQLsource());
        }
    }

    @Override
    protected Expression tryLiteral(ExpressionNode node) {
        return null;
    }

    @Override
    protected Expression literal(ConstantExpression expression) {
        if (expression.getAkType() == null)
            return Expressions.literal(expression.getValue());
        else
            return Expressions.literal(expression.getValue(), expression.getAkType());
    }

    @Override
    protected Expression variable(ParameterExpression expression) {
        return Expressions.variable(expression.getAkType(), expression.getPosition());
    }

    @Override
    protected Expression compare(Expression left, ComparisonCondition comparison, Expression right) {
        return Expressions.compare(left, comparison.getOperation(), right);
    }

    @Override
    protected Expression collate(Expression left, Comparison comparison, Expression right, AkCollator collator) {
        return Expressions.collate(left, comparison, right, collator);
    }

    @Override
    protected AkCollator collator(ComparisonCondition cond, Expression left, Expression right) {
        return ExpressionTypes.operationCollation(TypesTranslation.toExpressionType(cond.getLeft().getSQLtype()),
                                                  TypesTranslation.toExpressionType(cond.getRight().getSQLtype()));
    }

    @Override
    protected Expression in(Expression lhs, List<Expression> rhs, InListCondition inList) {
        return new InExpression(lhs, rhs);
    }

    @Override
    protected Expression assembleFieldExpression(RowType rowType, int fieldIndex) {
        return field(rowType, fieldIndex);
    }

    @Override
    protected Expression assembleBoundFieldExpression(RowType rowType, int rowIndex, int fieldIndex) {
        return boundField(rowType, rowIndex, fieldIndex);
    }

    @Override
    protected Expression assembleRoutine(ExpressionNode routineNode, 
                                         Routine routine,
                                         List<ExpressionNode> operandNodes,
                                         ColumnExpressionContext columnContext,
                                         SubqueryOperatorAssembler<Expression> subqueryAssembler) {
        List<Expression> inputs = assembleExpressions(operandNodes, columnContext, subqueryAssembler);
        switch (routine.getCallingConvention()) {
        case JAVA:
            return new ServerJavaMethodExpression(routine, inputs);
        case SCRIPT_FUNCTION_JAVA:
        case SCRIPT_FUNCTION_JSON:
            return new ScriptFunctionJavaRoutineExpression(routine, inputs);
        case SCRIPT_BINDINGS:
        case SCRIPT_BINDINGS_JSON:
            return new ScriptBindingsRoutineExpression(routine, inputs);
        default:
            throw new AkibanInternalException("Unimplemented routine " + routine);
        }
    }

    @Override
    public Operator assembleAggregates(Operator inputOperator, RowType rowType, int nkeys,
                                       AggregateSource aggregateSource)
    {
        List<String> names = aggregateSource.getAggregateFunctions();
        List<Object> options = aggregateSource.getOptions();
        return API.aggregate_Partial(
                inputOperator,
                rowType,
                nkeys,
                functionsRegistry,
                names,
                options);
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
