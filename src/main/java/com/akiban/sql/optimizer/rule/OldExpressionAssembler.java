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

import com.akiban.sql.optimizer.TypesTranslation;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.std.InExpression;
import com.akiban.server.expression.std.IntervalCastExpression;
import static com.akiban.server.expression.std.Expressions.*;
import com.akiban.server.service.functions.FunctionsRegistry;
import com.akiban.qp.rowtype.RowType;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Turn {@link ExpressionNode} into {@link Expression}. */
public class OldExpressionAssembler implements ExpressionAssembler<Expression>
{
    private static final Logger logger = LoggerFactory.getLogger(OldExpressionAssembler.class);

    private FunctionsRegistry functionsRegistry;

    public OldExpressionAssembler(RulesContext rulesContext) {
        this.functionsRegistry = ((SchemaRulesContext)
                                  rulesContext).getFunctionsRegistry();
    }

    public FunctionsRegistry getFunctionRegistry()
    {
        return functionsRegistry;
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
        public List<ColumnExpressionToIndex> getBoundRows();

        /** Get the index offset to be used for the deepest nested loop.
         * Normally this is the number of parameters to the query.
         */
        public int getExpressionBindingsOffset();

        /** Get the index offset to be used for the deepest nested loop.
         * These come after any expressions.
         */
        public int getLoopBindingsOffset();
    }

    public interface SubqueryOperatorAssembler<T> {
        /** Assemble the given subquery expression. */
        public T assembleSubqueryExpression(SubqueryExpression subqueryExpression);
    }

    @Override
    public Expression assembleExpression(ExpressionNode node,
                                         ColumnExpressionContext columnContext,
                                         SubqueryOperatorAssembler<Expression> subqueryAssembler) {
        if (node instanceof ConstantExpression) {
            if (node.getAkType() == null)
                return literal(((ConstantExpression)node).getValue());
            else
                return literal(((ConstantExpression)node).getValue(),
                               node.getAkType());
        }
        else if (node instanceof ColumnExpression)
            return assembleColumnExpression((ColumnExpression)node, columnContext);
        else if (node instanceof ParameterExpression)
            return variable(node.getAkType(), ((ParameterExpression)node).getPosition());
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
            return compare(assembleExpression(cond.getLeft(), 
                                              columnContext, subqueryAssembler),
                           cond.getOperation(),
                           assembleExpression(cond.getRight(), 
                                              columnContext, subqueryAssembler));
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
            Expression lhs = assembleExpression(inList.getOperand(), 
                                                columnContext, subqueryAssembler);
            List<Expression> rhs = assembleExpressions(inList.getExpressions(),
                                                       columnContext, subqueryAssembler);
            return new InExpression(lhs, rhs);
        }
        else if (node instanceof SubqueryExpression)
            return subqueryAssembler.assembleSubqueryExpression((SubqueryExpression)node);
        else if (node instanceof AggregateFunctionExpression)
            throw new UnsupportedSQLException("Aggregate used as regular function", 
                                              node.getSQLsource());
        else
            throw new UnsupportedSQLException("Unknown expression", node.getSQLsource());
    }

    public Expression assembleFunction(ExpressionNode functionNode,
                                       String functionName,
                                       List<ExpressionNode> argumentNodes,
                                       ColumnExpressionContext columnContext,
                                       SubqueryOperatorAssembler subqueryAssembler) {
        List<Expression> arguments = 
            assembleExpressions(argumentNodes, columnContext, subqueryAssembler);
        int nargs = arguments.size();
        List<ExpressionType> types = new ArrayList<ExpressionType>(nargs + 1);
        for (int i = 0; i < nargs; i++) {
            types.add(TypesTranslation.toExpressionType(argumentNodes.get(i).getSQLtype()));
        }
        types.add(TypesTranslation.toExpressionType(functionNode.getSQLtype()));
        return functionsRegistry.composer(functionName).compose(arguments, types);
    }
                                       
    public Expression assembleColumnExpression(ColumnExpression column,
                                               ColumnExpressionContext columnContext) {
        ColumnExpressionToIndex currentRow = columnContext.getCurrentRow();
        if (currentRow != null) {
            int fieldIndex = currentRow.getIndex(column);
            if (fieldIndex >= 0)
                return field(currentRow.getRowType(), fieldIndex);
        }
        
        List<ColumnExpressionToIndex> boundRows = columnContext.getBoundRows();
        for (int rowIndex = boundRows.size() - 1; rowIndex >= 0; rowIndex--) {
            ColumnExpressionToIndex boundRow = boundRows.get(rowIndex);
            if (boundRow == null) continue;
            int fieldIndex = boundRow.getIndex(column);
            if (fieldIndex >= 0) {
                rowIndex += columnContext.getLoopBindingsOffset();
                return boundField(boundRow.getRowType(), rowIndex, fieldIndex);
            }
        }
        logger.debug("Did not find {} from {} in {}", 
                     new Object[] { column, column.getTable(), boundRows });
        throw new AkibanInternalException("Column not found " + column);
    }

    protected Expression assembleCastExpression(CastExpression castExpression,
                                                ColumnExpressionContext columnContext,
                                                SubqueryOperatorAssembler subqueryAssembler) {
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
                expr = new com.akiban.server.expression.std.CastExpression(toType, expr);
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
                    expr = new com.akiban.server.expression.std.TruncateStringExpression(toSQL.getMaximumWidth(), expr);
            }
            break;
        case DECIMAL:
            {
                DataTypeDescriptor fromSQL = operand.getSQLtype();
                DataTypeDescriptor toSQL = castExpression.getSQLtype();
                if ((toSQL != null) && !toSQL.equals(fromSQL))
                    // Cast to DECIMAL scale.
                    expr = new com.akiban.server.expression.std.ScaleDecimalExpression(toSQL.getPrecision(), toSQL.getScale(), expr);
            }
            break;
        }
        return expr;
    }

    protected List<Expression> assembleExpressions(List<ExpressionNode> expressions,
                                                   ColumnExpressionContext columnContext,
                                                   SubqueryOperatorAssembler subqueryAssembler) {
        List<Expression> result = new ArrayList<Expression>(expressions.size());
        for (ExpressionNode expr : expressions) {
            result.add(assembleExpression(expr, columnContext, subqueryAssembler));
        }
        return result;
    }

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

}
