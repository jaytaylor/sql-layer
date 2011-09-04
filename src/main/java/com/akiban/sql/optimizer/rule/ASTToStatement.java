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
import com.akiban.sql.optimizer.plan.JoinNode;
import com.akiban.sql.optimizer.plan.ResultSet.ResultExpression;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.akiban.sql.optimizer.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;

import com.akiban.qp.physicaloperator.API.JoinType;

import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ParseException;
import com.akiban.server.error.UnsupportedSQLException;

import com.akiban.qp.expression.Comparison;

import java.util.*;

/** Turn a parsed SQL AST into this package's format.
 */
public class ASTToStatement extends BaseRule
{
    public PlanNode apply(PlanNode plan) {
        DMLStatementNode stmt = ((AST)plan).getStatement();
        try {
            return toStatement(stmt);
        }
        catch (StandardException ex) {
            throw new ParseException("", ex.getMessage(), "");
        }
    }

    /** Convert given statement into appropriate intermediate form. */
    protected BaseStatement toStatement(DMLStatementNode stmt) throws StandardException {
        switch (stmt.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
            return toSelectQuery((CursorNode)stmt);
        case NodeTypes.DELETE_NODE:
            return toDeleteStatement((DeleteNode)stmt);
        case NodeTypes.UPDATE_NODE:
            return toUpdateStatement((UpdateNode)stmt);
        case NodeTypes.INSERT_NODE:
            return toInsertStatement((InsertNode)stmt);
        default:
            throw new StandardException("Unsupported statement type: " +
                                        stmt.statementToString());
        }
    }

    // SELECT
    protected SelectQuery toSelectQuery(CursorNode cursorNode) 
            throws StandardException {
        PlanNode query = toQueryForSelect(cursorNode.getResultSetNode(),
                                          cursorNode.getOrderByList(),
                                          cursorNode.getOffsetClause(),
                                          cursorNode.getFetchFirstClause());
        if (cursorNode.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new UnsupportedSQLException("FOR UPDATE", cursorNode);
        return new SelectQuery(query);
    }

    // UPDATE
    protected UpdateStatement toUpdateStatement(UpdateNode updateNode)
            throws StandardException {
        ResultSetNode rs = updateNode.getResultSetNode();
        PlanNode query = toQuery((SelectNode)rs);
        TableNode targetTable = getTargetTable(updateNode);
        ResultColumnList rcl = rs.getResultColumns();
        List<UpdateColumn> updateColumns = 
            new ArrayList<UpdateColumn>(rcl.size());
        for (ResultColumn result : rcl) {
            Column column = getColumnReferenceColumn(result.getReference(),
                                                     "result column");
            ExpressionNode value = toExpression(result.getExpression());
            updateColumns.add(new UpdateColumn(column, value));
        }
        return new UpdateStatement(query, targetTable, updateColumns);
    }

    // INSERT
    protected InsertStatement toInsertStatement(InsertNode insertNode)
            throws StandardException {
        PlanNode query = toQueryForSelect(insertNode.getResultSetNode(),
                                          insertNode.getOrderByList(),
                                          insertNode.getOffset(),
                                          insertNode.getFetchFirst());
        TableNode targetTable = getTargetTable(insertNode);
        List<Column> targetColumns;
        ResultColumnList rcl = insertNode.getTargetColumnList();
        if (rcl != null) {
            targetColumns = new ArrayList<Column>(rcl.size());
            for (ResultColumn resultColumn : rcl) {
                Column column = getColumnReferenceColumn(resultColumn.getReference(),
                                                         "Unsupported target column");
                targetColumns.add(column);
            }
        }
        else {
            // No explicit column list: use DDL order.
            int ncols = insertNode.getResultSetNode().getResultColumns().size();
            List<Column> aisColumns = targetTable.getTable().getColumns();
            // TODO: Warning? Error?
            if (ncols > aisColumns.size())
                ncols = aisColumns.size();
            targetColumns = new ArrayList<Column>(ncols);
            for (int i = 0; i < ncols; i++) {
                targetColumns.add(aisColumns.get(i));
            }
        }
        return new InsertStatement(query, targetTable, targetColumns);
    }
    
    // DELETE
    protected DeleteStatement toDeleteStatement(DeleteNode deleteNode)
            throws StandardException {
        PlanNode query = toQuery((SelectNode)deleteNode.getResultSetNode());
        TableNode targetTable = getTargetTable(deleteNode);
        return new DeleteStatement(query, targetTable);
    }

    /** The query part of SELECT / INSERT, which might be VALUES / UNION */
    protected PlanNode toQueryForSelect(ResultSetNode resultSet,
                                        OrderByList orderByList,
                                        ValueNode offsetClause,
                                        ValueNode fetchFirstClause)
            throws StandardException {
        if (resultSet instanceof SelectNode)
            return toQueryForSelect((SelectNode)resultSet,
                                    orderByList,
                                    offsetClause,
                                    fetchFirstClause);
        else if (resultSet instanceof RowResultSetNode) {
            ResultColumnList resultColumns = resultSet.getResultColumns();
            List<ExpressionNode> row = new ArrayList<ExpressionNode>(resultColumns.size());
            for (ResultColumn resultColumn : resultColumns) {
                row.add(toExpression(resultColumn.getExpression()));
            }
            List<List<ExpressionNode>> rows = new ArrayList<List<ExpressionNode>>();
            rows.add(row);
            return new ExpressionsSource(rows);
        }
        else if (resultSet instanceof UnionNode) {
            UnionNode union = (UnionNode)resultSet;
            PlanNode left = toQueryForSelect(union.getLeftResultSet(), 
                                             null, null, null);
            PlanNode right = toQueryForSelect(union.getRightResultSet(), 
                                              null, null, null);
            boolean all = true; // TODO: union.isAll();
            if (all &&
                (left instanceof ExpressionsSource) &&
                (right instanceof ExpressionsSource)) {
                ExpressionsSource result = (ExpressionsSource)left;
                result.getExpressions().addAll(((ExpressionsSource)
                                                right).getExpressions());
                return result;
            }
            else {
                return new Union(left, right, all);
            }
        }
        else
            throw new UnsupportedSQLException("Unsupported query", resultSet);
    }

    /** A normal SELECT */
    protected PlanNode toQueryForSelect(SelectNode selectNode,
                                        OrderByList orderByList,
                                        ValueNode offsetClause,
                                        ValueNode fetchFirstClause)
            throws StandardException {
        PlanNode query = toQuery(selectNode);

        ResultColumnList rcl = selectNode.getResultColumns();
        List<ResultExpression> results = new ArrayList<ResultExpression>(rcl.size());
        for (ResultColumn result : rcl) {
            ExpressionNode expr = toExpression(result.getExpression());
            String name = result.getName();
            boolean nameDefaulted =
                (result.getExpression() instanceof ColumnReference) &&
                (name == ((ColumnReference)result.getExpression()).getColumnName());
            ResultExpression rexpr = new ResultExpression(expr, name, nameDefaulted);
            results.add(rexpr);
        }

        List<OrderByExpression> sorts = new ArrayList<OrderByExpression>();
        if (orderByList != null) {
            for (OrderByColumn orderByColumn : orderByList) {
                ExpressionNode expression = toExpression(orderByColumn.getExpression());
                // If column has a constant value, there is no need to sort on it.
                if (!expression.isConstant())
                    sorts.add(new OrderByExpression(expression, 
                                                  orderByColumn.isAscending()));
            }
        }

        // Stupid but legal: 
        //  SELECT 1 FROM t ORDER BY MAX(c); 
        //  SELECT 1 FROM t HAVING MAX(c) > 0; 
        if ((selectNode.getGroupByList() != null) ||
            (selectNode.getHavingClause() != null) ||
            hasAggregateFunctionA(results) ||
            hasAggregateFunctionA(sorts)) {

            query = toAggregateSource(query, selectNode.getGroupByList());

            List<ConditionExpression> conditions = 
                toConditions(selectNode.getHavingClause());
            if (conditions != null)
                query = new Filter(query, conditions);
        }

        if (!sorts.isEmpty()) {
            query = new Sort(query, sorts);
        }

        query = new ResultSet(query, results);

        if (selectNode.hasWindows())
            throw new UnsupportedSQLException("WINDOW", selectNode);
        
        if (selectNode.isDistinct())
            query = new Distinct(query);

        if ((offsetClause != null) || 
            (fetchFirstClause != null))
            query = toLimit(query, offsetClause, fetchFirstClause);

        return query;
    }

    /** The common top-level filtered joins part of all statements. */
    protected PlanNode toQuery(SelectNode selectNode)
            throws StandardException {
        Joinable joins = null;
        for (FromTable fromTable : selectNode.getFromList()) {
            if (joins == null)
                joins = toJoinNode(fromTable);
            else
                joins = joinNodes(joins, toJoinNode(fromTable), JoinType.INNER_JOIN);
        }
        PlanNode query = joins;
        List<ConditionExpression> conditions = 
            toConditions(selectNode.getWhereClause());
        if (conditions != null) {
            if (hasAggregateFunction(conditions))
                throw new UnsupportedSQLException("Aggregate not allowed in WHERE",
                                                  selectNode.getWhereClause());
            query = new Filter(query, conditions);
        }
        return query;
    }

    protected Map<FromTable,Joinable> joinNodes =
        new HashMap<FromTable,Joinable>();

    protected Joinable toJoinNode(FromTable fromTable)
            throws StandardException {
        Joinable result;
        if (fromTable instanceof FromBaseTable) {
            TableBinding tb = (TableBinding)fromTable.getUserData();
            if (tb == null)
                throw new UnsupportedSQLException("FROM table",
                                                  fromTable);
            TableNode table = getTableNode((UserTable)tb.getTable());
            result = new TableSource(table);
        }
        else if (fromTable instanceof com.akiban.sql.parser.JoinNode) {
            com.akiban.sql.parser.JoinNode joinNode = 
                (com.akiban.sql.parser.JoinNode)fromTable;
            JoinType joinType;
            switch (joinNode.getNodeType()) {
            case NodeTypes.JOIN_NODE:
                joinType = JoinType.INNER_JOIN;
                break;
            case NodeTypes.HALF_OUTER_JOIN_NODE:
                if (((HalfOuterJoinNode)joinNode).isRightOuterJoin())
                    joinType = JoinType.RIGHT_JOIN;
                else
                    joinType = JoinType.LEFT_JOIN;
                break;
            default:
                throw new UnsupportedSQLException("Unsupported join type", joinNode);
            }
            JoinNode join = joinNodes(toJoinNode((FromTable)joinNode.getLeftResultSet()),
                                      toJoinNode((FromTable)joinNode.getRightResultSet()),
                                      joinType);
            join.setJoinConditions(toConditions(joinNode.getJoinClause()));
            result = join;
        }
        else
            throw new UnsupportedSQLException("Unsupported FROM non-table", fromTable);
        joinNodes.put(fromTable, result);
        return result;
    }

    protected JoinNode joinNodes(Joinable left, Joinable right, JoinType joinType)
            throws StandardException {
        return new JoinNode(left, right, joinType);
    }

    /** Add a set of conditions to input. */
    protected List<ConditionExpression> toConditions(ValueNode cnfClause)
            throws StandardException {
        List<ConditionExpression> conditions = new ArrayList<ConditionExpression>();
        while (cnfClause != null) {
            if (cnfClause.isBooleanTrue()) break;
            if (!(cnfClause instanceof AndNode))
                throw new UnsupportedSQLException("Unsupported complex WHERE",
                                                  cnfClause);
            AndNode andNode = (AndNode)cnfClause;
            cnfClause = andNode.getRightOperand();
            ValueNode condition = andNode.getLeftOperand();
            addCondition(conditions, condition);
        }
        if (conditions.isEmpty())
            return null;
        else
            return conditions;
    }

    protected void addCondition(List<ConditionExpression> conditions, ValueNode condition)
            throws StandardException {
        switch (condition.getNodeType()) {
        case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.EQ);
            break;
        case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.GT);
            break;
        case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.GE);
            break;
        case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.LT);
            break;
        case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.LE);
            break;
        case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
            addComparisonCondition(conditions,
                                   (BinaryOperatorNode)condition, Comparison.NE);
            break;
        case NodeTypes.BETWEEN_OPERATOR_NODE:
            addBetweenCondition(conditions,
                                (BetweenOperatorNode)condition);
            break;
        case NodeTypes.IN_LIST_OPERATOR_NODE:
            addInCondition(conditions,
                           (InListOperatorNode)condition);
            break;

        case NodeTypes.IS_NULL_NODE:
        case NodeTypes.IS_NOT_NULL_NODE:
            addFunctionCondition(conditions,
                                 (UnaryOperatorNode)condition);
            break;
        case NodeTypes.LIKE_OPERATOR_NODE:
            addFunctionCondition(conditions,
                                 (TernaryOperatorNode)condition);
            break;

        case NodeTypes.BOOLEAN_CONSTANT_NODE:
            if (condition.isBooleanTrue())
                break;
            /* else falls through */
        default:
            // TODO: Rest of these.
            throw new UnsupportedSQLException("Unsupported WHERE predicate",
                                              condition);
        }
    }

    protected void addComparisonCondition(List<ConditionExpression> conditions,
                                          BinaryOperatorNode binop, Comparison op)
            throws StandardException {
        ExpressionNode left = toExpression(binop.getLeftOperand());
        ExpressionNode right = toExpression(binop.getRightOperand());
        conditions.add(new ComparisonCondition(op, left, right,
                                               binop.getType(), binop));
    }

    protected void addBetweenCondition(List<ConditionExpression> conditions,
                                       BetweenOperatorNode between)
            throws StandardException {
        ExpressionNode left = toExpression(between.getLeftOperand());
        ValueNodeList rightOperandList = between.getRightOperandList();
        ExpressionNode right1 = toExpression(rightOperandList.get(0));
        ExpressionNode right2 = toExpression(rightOperandList.get(1));
        DataTypeDescriptor type = between.getType();
        conditions.add(new ComparisonCondition(Comparison.GE, left, right1, type, null));
        conditions.add(new ComparisonCondition(Comparison.LE, left, right2, type, null));
    }

    protected void addInCondition(List<ConditionExpression> conditions,
                                  InListOperatorNode in)
            throws StandardException {
        ExpressionNode left = toExpression(in.getLeftOperand());
        ValueNodeList rightOperandList = in.getRightOperandList();
        // TODO: For real now.
        if (rightOperandList.size() != 1)
            throw new UnsupportedSQLException("IN predicate", in);
        ExpressionNode right1 = toExpression(rightOperandList.get(0));
        conditions.add(new ComparisonCondition(Comparison.EQ, left, right1,
                                               in.getType(), in));
    }
    
    protected void addFunctionCondition(List<ConditionExpression> conditions,
                                        UnaryOperatorNode unary)
            throws StandardException {
        List<ExpressionNode> operands = new ArrayList<ExpressionNode>(1);
        operands.add(toExpression(unary.getOperand()));
        conditions.add(new FunctionCondition(unary.getMethodName(),
                                             operands,
                                             unary.getType(), unary));
    }

    protected void addFunctionCondition(List<ConditionExpression> conditions,
                                        BinaryOperatorNode binary)
            throws StandardException {
        List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
        operands.add(toExpression(binary.getLeftOperand()));
        operands.add(toExpression(binary.getRightOperand()));
        conditions.add(new FunctionCondition(binary.getMethodName(),
                                             operands,
                                             binary.getType(), binary));
    }

    protected void addFunctionCondition(List<ConditionExpression> conditions,
                                        TernaryOperatorNode ternary)
            throws StandardException {
        List<ExpressionNode> operands = new ArrayList<ExpressionNode>(3);
        operands.add(toExpression(ternary.getReceiver()));
        operands.add(toExpression(ternary.getLeftOperand()));
        operands.add(toExpression(ternary.getRightOperand()));
        conditions.add(new FunctionCondition(ternary.getMethodName(),
                                             operands,
                                             ternary.getType(), ternary));
    }

    /** LIMIT / OFFSET */
    protected Limit toLimit(PlanNode input, 
                            ValueNode offsetClause, 
                            ValueNode limitClause)
            throws StandardException {
        int offset = 0, limit = -1;
        boolean offsetIsParameter = false, limitIsParameter = false;
        if (offsetClause != null) {
            if (offsetClause instanceof ParameterNode) {
                offset = ((ParameterNode)offsetClause).getParameterNumber();
                offsetIsParameter = true;
            }
            else {
                offset = getIntegerConstant(offsetClause, 
                                           "OFFSET must be constant integer");
                if (offset < 0)
                    throw new UnsupportedSQLException("OFFSET must not be negative", 
                                                      offsetClause);
            }
        }
        if (limitClause != null) {
            if (limitClause instanceof ParameterNode) {
                limit = ((ParameterNode)limitClause).getParameterNumber();
                limitIsParameter = true;
            }
            else {
                limit = getIntegerConstant(offsetClause, 
                                           "LIMIT must be constant integer");
                if (limit < 0)
                    throw new UnsupportedSQLException("LIMIT must not be negative", 
                                                      limitClause);
            }
        }
        return new Limit(input, 
                         offset, offsetIsParameter,
                         limit, limitIsParameter);
    }

    protected TableNode getTargetTable(DMLModStatementNode statement)
            throws StandardException {
        TableName tableName = statement.getTargetTableName();
        UserTable table = (UserTable)tableName.getUserData();
        if (table == null)
            throw new NoSuchTableException(tableName.getSchemaName(), 
                                           tableName.getTableName());
        return getTableNode(table);
    }
    
    protected Map<Group,TableTree> groups = new HashMap<Group,TableTree>();

    protected TableNode getTableNode(UserTable table)
            throws StandardException {
        Group group = table.getGroup();
        TableTree tables = groups.get(group);
        if (tables == null) {
            tables = new TableTree();
            groups.put(group, tables);
        }
        return tables.addNode(table);
    }

    protected TableNode getColumnTableNode(Column column)
            throws StandardException {
        return getTableNode(column.getUserTable());
    }

    /** Translate expression to intermediate form. */
    protected ExpressionNode toExpression(ValueNode valueNode)
            throws StandardException {
        if (valueNode == null) {
            return new ConstantExpression(null, null, null);
        }
        DataTypeDescriptor type = valueNode.getType();
        if (valueNode instanceof ColumnReference) {
            ColumnBinding cb = (ColumnBinding)((ColumnReference)valueNode).getUserData();
            if (cb == null)
                throw new UnsupportedSQLException("Unsupported column", valueNode);
            Joinable joinNode = joinNodes.get(cb.getFromTable());
            if (!(joinNode instanceof ColumnSource))
                throw new UnsupportedSQLException("Unsupported column", valueNode);
            Column column = cb.getColumn();
            if (column != null)
                return new ColumnExpression(((TableSource)joinNode), column, 
                                            type, valueNode);
            else
                // TODO: Get the position correctly from reference.
                return new ColumnExpression(((ColumnSource)joinNode), -1, 
                                            type, valueNode);
        }
        else if (valueNode instanceof ConstantNode)
            return new ConstantExpression(((ConstantNode)valueNode).getValue(), 
                                          type, valueNode);
        else if (valueNode instanceof ParameterNode)
            return new ParameterExpression(((ParameterNode)valueNode)
                                           .getParameterNumber(),
                                           type, valueNode);
        else if (valueNode instanceof CastNode)
            return new CastExpression(toExpression(((CastNode)valueNode)
                                                   .getCastOperand()),
                                      type, valueNode);
        else if (valueNode instanceof AggregateNode) {
            AggregateNode aggregateNode = (AggregateNode)valueNode;
            String function = aggregateNode.getAggregateName();
            ExpressionNode operand = null;
            if ("COUNT(*)".equals(function)) {
                function = "COUNT";
            }
            else {
                operand = toExpression(aggregateNode.getOperand());
                if (hasAggregateFunction(operand)) {
                    throw new UnsupportedSQLException("Cannot nest aggregate functions",
                                                      aggregateNode);
                }
            }
            return new AggregateFunctionExpression(function,
                                                   operand,
                                                   aggregateNode.isDistinct(),
                                                   type, valueNode);
        }
        else if (valueNode instanceof UnaryOperatorNode) {
            UnaryOperatorNode unary = (UnaryOperatorNode)valueNode;
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>(1);
            operands.add(toExpression(unary.getOperand()));
            return new FunctionExpression(unary.getMethodName(),
                                          operands,
                                          unary.getType(), unary);
        }
        else if (valueNode instanceof BinaryOperatorNode) {
            BinaryOperatorNode binary = (BinaryOperatorNode)valueNode;
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>(2);
            operands.add(toExpression(binary.getLeftOperand()));
            operands.add(toExpression(binary.getRightOperand()));
            return new FunctionExpression(binary.getMethodName(),
                                          operands,
                                          binary.getType(), binary);
        }
        else if (valueNode instanceof TernaryOperatorNode) {
            TernaryOperatorNode ternary = (TernaryOperatorNode)valueNode;
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>(3);
            operands.add(toExpression(ternary.getReceiver()));
            operands.add(toExpression(ternary.getLeftOperand()));
            operands.add(toExpression(ternary.getRightOperand()));
            return new FunctionExpression(ternary.getMethodName(),
                                          operands,
                                          ternary.getType(), ternary);
        }
        else if (valueNode instanceof JavaToSQLValueNode) {
            return toExpression(((JavaToSQLValueNode)valueNode).getJavaValueNode(),
                                valueNode);
        }
        else
            throw new UnsupportedSQLException("Unsupported operand", valueNode);
    }

    // TODO: Need to figure out return type.  Maybe better to have
    // done this earlier and bound to a known function and elided the
    // Java stuff then.
    protected ExpressionNode toExpression(JavaValueNode javaToSQL,
                                          ValueNode valueNode)
            throws StandardException {
        if (javaToSQL instanceof MethodCallNode) {
            MethodCallNode methodCall = (MethodCallNode)javaToSQL;
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>();
            if (methodCall.getMethodParameters() != null) {
                for (JavaValueNode javaValue : methodCall.getMethodParameters()) {
                    operands.add(toExpression(javaValue, null));
                }
            }
            return new FunctionExpression(methodCall.getMethodName(),
                                          operands,
                                          valueNode.getType(), valueNode);
        }
        else if (javaToSQL instanceof SQLToJavaValueNode) {
            return toExpression(((SQLToJavaValueNode)javaToSQL).getSQLValueNode());
        }
        else 
            throw new UnsupportedSQLException("Unsupported operand", valueNode);
    }

    /** Get the column that this node references or else return null
     * or throw given error.
     */
    protected Column getColumnReferenceColumn(ValueNode value, String errmsg)
            throws StandardException {
        if (value instanceof ColumnReference) {
            ColumnReference cref = (ColumnReference)value;
            ColumnBinding cb = (ColumnBinding)cref.getUserData();
            if (cb != null) {
                Column column = cb.getColumn();
                if (column != null)
                    return column;
            }
        }
        if (errmsg == null)
            return null;
        throw new UnsupportedSQLException(errmsg, value);
    }

    /** Get the constant integer value that this node represents or else throw error. */
    protected int getIntegerConstant(ValueNode value, String errmsg) {
        if (value instanceof NumericConstantNode) {
            Object number = ((NumericConstantNode)value).getValue();
            if (number instanceof Integer)
                return ((Integer)number).intValue();
        }
        throw new UnsupportedSQLException(errmsg, value);
    }

    /** Construct an aggregating node.
     * This only sets the skeleton with the group by fields. Later,
     * aggregate functions from the result columns, HAVING & ORDER BY
     * clauses will be added there and the result column adjusted to
     * reflect this.
     */
    protected AggregateSource toAggregateSource(PlanNode input,
                                                GroupByList groupByList)
            throws StandardException {
        List<ExpressionNode> groupBy = new ArrayList<ExpressionNode>();
        if (groupByList != null) {
            for (GroupByColumn groupByColumn : groupByList) {
                groupBy.add(toExpression(groupByColumn.getColumnExpression()));
            }
        }
        return new AggregateSource(input, groupBy);
    }
    
    /** Does any element include an aggregate function? */
    protected boolean hasAggregateFunction(Collection<? extends ExpressionNode> c) {
        for (ExpressionNode expr : c) {
            if (hasAggregateFunction(expr))
                return true;
        }
        return false;
    }

    /** Does any element include an aggregate function? */
    protected boolean hasAggregateFunctionA(Collection<? extends AnnotatedExpression> c) {
        for (AnnotatedExpression aexpr : c) {
            if (hasAggregateFunction(aexpr.getExpression()))
                return true;
        }
        return false;
    }

    /** Does this expression include any aggregates? */
    protected boolean hasAggregateFunction(ExpressionNode expr) {
        // TODO: Walk the tree.
        return (expr instanceof AggregateFunctionExpression);
    }

}
