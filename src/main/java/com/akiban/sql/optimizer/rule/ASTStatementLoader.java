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

import com.akiban.server.expression.std.Comparison;
import com.akiban.server.types.AkType;
import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;
import static com.akiban.sql.optimizer.plan.PlanContext.*;
import com.akiban.sql.optimizer.plan.ResultSet.ResultField;
import com.akiban.sql.optimizer.plan.Sort.OrderByExpression;
import com.akiban.sql.optimizer.plan.UpdateStatement.UpdateColumn;

import com.akiban.sql.optimizer.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;

import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ParseException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.server.error.OrderGroupByNonIntegerConstant;
import com.akiban.server.error.OrderGroupByIntegerOutOfRange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Turn a parsed SQL AST into this package's format.
 */
public class ASTStatementLoader extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(ASTStatementLoader.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    public static final WhiteboardMarker<AST> MARKER = 
        new DefaultWhiteboardMarker<AST>();

    /** Recover the {@link AST} put on the whiteboard when loaded. */
    public static AST getAST(PlanContext plan) {
        return plan.getWhiteboard(MARKER);
    }

    @Override
    public void apply(PlanContext plan) {
        AST ast = (AST)plan.getPlan();
        plan.putWhiteboard(MARKER, ast);
        DMLStatementNode stmt = ast.getStatement();
        try {
            plan.setPlan(new Loader().toStatement(stmt));
        }
        catch (StandardException ex) {
            // TODO: Separate out Parser subsystem error from true parse error.
            throw new ParseException("", ex.getMessage(), "");
        }
    }

    static class Loader {
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
            if (query instanceof ResultSet)
                query = ((ResultSet)query).getInput();
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
                List<ExpressionNode> row = toExpressionsRow(resultSet);
                List<List<ExpressionNode>> rows = new ArrayList<List<ExpressionNode>>();
                rows.add(row);
                return new ExpressionsSource(rows);
            }
            else if (resultSet instanceof UnionNode) {
                UnionNode union = (UnionNode)resultSet;
                boolean all = union.isAll();
                PlanNode left = toQueryForSelect(union.getLeftResultSet());
                if (all &&
                    (left instanceof ExpressionsSource) &&
                    ((union.getRightResultSet() instanceof RowResultSetNode) ||
                     (union.getRightResultSet() instanceof UnionNode)))
                    return addMoreExpressions((ExpressionsSource)left, union);
                PlanNode right = toQueryForSelect(union.getRightResultSet());
                return new Union(left, right, all);
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
            List<ExpressionNode> projects = new ArrayList<ExpressionNode>(rcl.size());
            List<ResultField> results = new ArrayList<ResultField>(rcl.size());
            for (ResultColumn result : rcl) {
                String name = result.getName();
                DataTypeDescriptor type = result.getType();
                boolean nameDefaulted =
                    (result.getExpression() instanceof ColumnReference) &&
                    (name == ((ColumnReference)result.getExpression()).getColumnName());
                Column column = null;
                ExpressionNode expr = toExpression(result.getExpression());
                projects.add(expr);
                if (expr instanceof ColumnExpression) {
                    column = ((ColumnExpression)expr).getColumn();
                    if ((column != null) && nameDefaulted)
                        name = column.getName();
                }
                results.add(new ResultField(name, type, column));
            }

            List<OrderByExpression> sorts = new ArrayList<OrderByExpression>();
            if (orderByList != null) {
                for (OrderByColumn orderByColumn : orderByList) {
                    ExpressionNode expression = toOrderGroupBy(orderByColumn.getExpression(), projects, "ORDER");
                    sorts.add(new OrderByExpression(expression,
                                                    orderByColumn.isAscending()));
                }
            }

            // Stupid but legal: 
            //  SELECT 1 FROM t ORDER BY MAX(c); 
            //  SELECT 1 FROM t HAVING MAX(c) > 0; 
            if ((selectNode.getGroupByList() != null) ||
                (selectNode.getHavingClause() != null) ||
                hasAggregateFunction(projects) ||
                hasAggregateFunctionA(sorts)) {
                query = toAggregateSource(query, selectNode.getGroupByList(), projects);
                query = new Select(query, toConditions(selectNode.getHavingClause()));
            }

            if (selectNode.hasWindows())
                throw new UnsupportedSQLException("WINDOW", selectNode);
        
            if (selectNode.isDistinct()) {
                Project project = new Project(query, projects);
                query = project;
                if (!sorts.isEmpty()) {
                    query = new Sort(query, sortsForDistinct(sorts, project));
                    query = new Distinct(query, Distinct.Implementation.PRESORTED);
                }
                else
                    query = new Distinct(query);
                if ((offsetClause != null) || 
                    (fetchFirstClause != null))
                    query = toLimit(query, offsetClause, fetchFirstClause);
            }
            else {
                if (!sorts.isEmpty()) {
                    query = new Sort(query, sorts);
                }
                if ((offsetClause != null) || 
                    (fetchFirstClause != null))
                    query = toLimit(query, offsetClause, fetchFirstClause);
                query = new Project(query, projects);
            }

            query = new ResultSet(query, results);

            return query;
        }

        protected PlanNode toQueryForSelect(ResultSetNode resultSet)
                throws StandardException {
            return toQueryForSelect(resultSet, null, null, null);
        }

        protected List<ExpressionNode> toExpressionsRow(ResultSetNode resultSet)
                throws StandardException {
            ResultColumnList resultColumns = resultSet.getResultColumns();
            List<ExpressionNode> row = new ArrayList<ExpressionNode>(resultColumns.size());
            for (ResultColumn resultColumn : resultColumns) {
                row.add(toExpression(resultColumn.getExpression()));
            }
            return row;
        }

        /** If start with VALUES, handle more of them right recursively
         * without using the stack. */
        protected PlanNode addMoreExpressions(ExpressionsSource into,
                                              UnionNode union) throws StandardException {
            while (true) {
                // The left is already in and this is a UNION ALL.
                if (union.getRightResultSet() instanceof RowResultSetNode) {
                    // Last row.
                    into.getExpressions().add(toExpressionsRow(union.getRightResultSet()));
                    return into;
                }
                if (!(union.getRightResultSet() instanceof UnionNode)) {
                    return new Union(into, 
                                     toQueryForSelect(union.getRightResultSet()), 
                                     true);
                }
                union = (UnionNode)union.getRightResultSet();
                if (!union.isAll() ||
                    !((union.getLeftResultSet() instanceof RowResultSetNode))) {
                    return new Union(into,
                                     toQueryForSelect(union),
                                     true);
                }
                into.getExpressions().add(toExpressionsRow(union.getLeftResultSet()));
            }
        }

        /** The common top-level join and select part of all statements. */
        protected PlanNode toQuery(SelectNode selectNode)
                throws StandardException {
            Joinable joins = null;
            for (FromTable fromTable : selectNode.getFromList()) {
                if (joins == null)
                    joins = toJoinNode(fromTable, true);
                else
                    joins = joinNodes(joins, toJoinNode(fromTable, true), JoinType.INNER);
            }
            ConditionList conditions = toConditions(selectNode.getWhereClause());
            if (hasAggregateFunction(conditions))
                throw new UnsupportedSQLException("Aggregate not allowed in WHERE",
                                                  selectNode.getWhereClause());
            return new Select(joins, conditions);
        }

        protected Map<FromTable,Joinable> joinNodes =
            new HashMap<FromTable,Joinable>();

        protected Joinable toJoinNode(FromTable fromTable, boolean required)
                throws StandardException {
            Joinable result;
            if (fromTable instanceof FromBaseTable) {
                TableBinding tb = (TableBinding)fromTable.getUserData();
                if (tb == null)
                    throw new UnsupportedSQLException("FROM table",
                                                      fromTable);
                TableNode table = getTableNode((UserTable)tb.getTable());
                result = new TableSource(table, required);
            }
            else if (fromTable instanceof com.akiban.sql.parser.JoinNode) {
                com.akiban.sql.parser.JoinNode joinNode = 
                    (com.akiban.sql.parser.JoinNode)fromTable;
                JoinType joinType;
                switch (joinNode.getNodeType()) {
                case NodeTypes.JOIN_NODE:
                    joinType = JoinType.INNER;
                    break;
                case NodeTypes.HALF_OUTER_JOIN_NODE:
                    if (((HalfOuterJoinNode)joinNode).isRightOuterJoin())
                        joinType = JoinType.RIGHT;
                    else
                        joinType = JoinType.LEFT;
                    break;
                default:
                    throw new UnsupportedSQLException("Unsupported join type", joinNode);
                }
                JoinNode join = joinNodes(toJoinNode((FromTable)joinNode.getLeftResultSet(),
                                                     required && (joinType != JoinType.RIGHT)),
                                          toJoinNode((FromTable)joinNode.getRightResultSet(),
                                                     required && (joinType != JoinType.LEFT)),
                                          joinType);
                join.setJoinConditions(toConditions(joinNode.getJoinClause()));
                result = join;
            }
            else if (fromTable instanceof FromSubquery) {
                FromSubquery fromSubquery = (FromSubquery)fromTable;
                PlanNode subquery = toQueryForSelect(fromSubquery.getSubquery(),
                                                     fromSubquery.getOrderByList(),
                                                     fromSubquery.getOffset(),
                                                     fromSubquery.getFetchFirst());
                result = new SubquerySource(new Subquery(subquery), 
                                            fromSubquery.getExposedName());
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
        protected ConditionList toConditions(ValueNode cnfClause)
                throws StandardException {
            ConditionList conditions = new ConditionList();
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
            return conditions;
        }

        /** Fill the given list with conditions from given AST node.
         * Takes a list because BETWEEN generates <em>two</em> conditions.
         */
        protected void addCondition(List<ConditionExpression> conditions, 
                                    ValueNode condition)
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

            case NodeTypes.SUBQUERY_NODE:
                addSubqueryCondition(conditions,
                                     (SubqueryNode)condition);
                break;

            case NodeTypes.LIKE_OPERATOR_NODE:
                addFunctionCondition(conditions,
                                     (TernaryOperatorNode)condition);
                break;
            case NodeTypes.IS_NULL_NODE:
            case NodeTypes.IS_NOT_NULL_NODE:
                addIsNullCondition(conditions,
                                   (IsNullNode)condition);
                break;

            case NodeTypes.IS_NODE:
                addIsCondition(conditions,
                               (IsNode)condition);
                break;

            case NodeTypes.OR_NODE:
            case NodeTypes.AND_NODE:
            case NodeTypes.NOT_NODE:
                addLogicalFunctionCondition(conditions, condition);
                break;

            case NodeTypes.BOOLEAN_CONSTANT_NODE:
                if (!condition.isBooleanTrue()) {
                    // FALSE = TRUE
                    conditions.add(new ComparisonCondition(Comparison.EQ,
                                                           toExpression(condition),
                                                           new BooleanConstantExpression(Boolean.TRUE),
                                                           condition.getType(), condition));
                }
                break;
            case NodeTypes.CAST_NODE:
                assert condition.getType().getTypeId().isBooleanTypeId();
                conditions.add(new BooleanCastExpression(toExpression(((CastNode)condition)
                                                                      .getCastOperand()),
                                                         condition.getType(), condition));
                break;
            case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
                conditions.add((ConditionExpression)
                               toExpression(((JavaToSQLValueNode)condition).getJavaValueNode(), 
                                            condition, true));
                break;
            default:
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
            if (rightOperandList.size() == 1) {
                ExpressionNode right1 = toExpression(rightOperandList.get(0));
                conditions.add(new ComparisonCondition(Comparison.EQ, left, right1,
                                                       in.getType(), in));
                return;
            }
            List<List<ExpressionNode>> rows = new ArrayList<List<ExpressionNode>>();
            for (ValueNode rightOperand : rightOperandList) {
                List<ExpressionNode> row = new ArrayList<ExpressionNode>(1);
                row.add(toExpression(rightOperand));
                rows.add(row);
            }
            ExpressionsSource source = new ExpressionsSource(rows);
            ConditionExpression cond = new ComparisonCondition(Comparison.EQ, left,
                                                               new ColumnExpression(source, 0,
                                                                                    left.getSQLtype(), null),
                                                               in.getType(), null);
            List<ExpressionNode> fields = new ArrayList<ExpressionNode>(1);
            fields.add(cond);
            PlanNode subquery = new Project(source, fields);
            conditions.add(new AnyCondition(new Subquery(subquery), in.getType(), in));
        }
    
        protected void addSubqueryCondition(List<ConditionExpression> conditions, 
                                            SubqueryNode subqueryNode)
                throws StandardException {
            PlanNode subquery = toQueryForSelect(subqueryNode.getResultSet(),
                                                 subqueryNode.getOrderByList(),
                                                 subqueryNode.getOffset(),
                                                 subqueryNode.getFetchFirst());
            boolean negate = false;
            Comparison comp = Comparison.EQ;
            ExpressionNode operand = null;
            boolean needOperand = false;
            ConditionList innerConds = null;
            switch (subqueryNode.getSubqueryType()) {
            case EXISTS:
                break;
            case NOT_EXISTS:
                negate = true;
                break;
            case IN:
            case EQ_ANY: 
                needOperand = true;
                break;
            case EQ_ALL: 
                negate = true;
                comp = Comparison.NE;
                needOperand = true;
                break;
            case NOT_IN: 
            case NE_ANY: 
                comp = Comparison.NE;
                needOperand = true;
                break;
            case NE_ALL: 
                negate = true;
                comp = Comparison.EQ;
                needOperand = true;
                break;
            case GT_ANY: 
                comp = Comparison.GT;
                needOperand = true;
                break;
            case GT_ALL: 
                negate = true;
                comp = Comparison.LE;
                needOperand = true;
                break;
            case GE_ANY: 
                comp = Comparison.GE;
                needOperand = true;
                break;
            case GE_ALL: 
                negate = true;
                comp = Comparison.LT;
                needOperand = true;
                break;
            case LT_ANY: 
                comp = Comparison.LT;
                needOperand = true;
                break;
            case LT_ALL: 
                negate = true;
                comp = Comparison.GE;
                needOperand = true;
                break;
            case LE_ANY: 
                comp = Comparison.LE;
                needOperand = true;
                break;
            case LE_ALL: 
                negate = true;
                comp = Comparison.GT;
                needOperand = true;
                break;
            }
            boolean distinct = false;
            // TODO: This may not be right for c IN (SELECT x ... UNION SELECT y ...).
            // Maybe turn that into an OR and optimize each separately.
            {
                PlanWithInput prev = null;
                PlanNode plan = subquery;
                while (true) {
                    if (!(plan instanceof BasePlanWithInput))
                        break;
                    PlanNode next = ((BasePlanWithInput)plan).getInput();
                    if (plan instanceof Project) {
                        Project project = (Project)plan;
                        if (needOperand) {
                            if (project.getFields().size() != 1)
                                throw new UnsupportedSQLException("Subquery must have exactly one column", subqueryNode);
                            operand = project.getFields().get(0);
                        }
                        // Don't need project any more.
                        if (prev != null)
                            prev.replaceInput(plan, next);
                        else
                            subquery = next;
                        break;
                    }
                    if (plan instanceof Distinct) {
                        distinct = true;
                    }
                    else if (!(plan instanceof ResultSet)) { // Also remove ResultSet.
                        prev = (PlanWithInput)plan;
                    }
                    plan = next;
                }
            }
            ConditionExpression condition;
            if (needOperand) {
                assert (operand != null);
                ExpressionNode left = toExpression(subqueryNode.getLeftOperand());
                ConditionExpression inner = new ComparisonCondition(comp, left, operand,
                                                                    subqueryNode.getType(), 
                                                                    subqueryNode);
                // We take this condition back off from the top of the
                // physical plan and move it to the expression, but it's
                // easier to think about the scoping as evaluated at the
                // end of the inner query.
                List<ExpressionNode> fields = new ArrayList<ExpressionNode>(1);
                fields.add(inner);
                subquery = new Project(subquery, fields);
                if (distinct)
                    // See InConditionReverser#convert(Select,AnyCondition).
                    subquery = new Distinct(subquery);
                condition = new AnyCondition(new Subquery(subquery), 
                                             subqueryNode.getType(), subqueryNode);
            }
            else {
                condition = new ExistsCondition(new Subquery(subquery), 
                                                subqueryNode.getType(), subqueryNode);
            }
            if (negate) {
                List<ConditionExpression> operands = new ArrayList<ConditionExpression>(1);
                operands.add(condition);
                condition = new LogicalFunctionCondition("not", 
                                                         operands,
                                                         subqueryNode.getType(), 
                                                         subqueryNode);
            }
            conditions.add(condition);
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

        protected void addIsNullCondition(List<ConditionExpression> conditions,
                                          IsNullNode isNull)
                throws StandardException {
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>(1);
            operands.add(toExpression(isNull.getOperand()));
            String function = isNull.getMethodName();
            boolean negated = false;
            if ("isNotNull".equals(function)) {
                function = "isNull";
                negated = true;
            }
            FunctionCondition cond = new FunctionCondition(function, operands,
                                                           isNull.getType(), isNull);
            if (negated) {
                List<ConditionExpression> noperands = new ArrayList<ConditionExpression>(1);
                noperands.add(cond);
                cond = new LogicalFunctionCondition("not", noperands,
                                                    isNull.getType(), isNull);
            }
            conditions.add(cond);
        }

        protected void addIsCondition(List<ConditionExpression> conditions,
                                      IsNode is)
                throws StandardException {
            List<ExpressionNode> operands = new ArrayList<ExpressionNode>(1);
            operands.add(toExpression(is.getLeftOperand()));
            String function;
            Boolean value = (Boolean)((ConstantNode)is.getRightOperand()).getValue();
            if (value == null)
                function = "isNull"; // No separate isUnknown.
            else if (value.booleanValue())
                function = "isTrue";
            else
                function = "isFalse";
            FunctionCondition cond = new FunctionCondition(function, operands,
                                                           is.getType(), is);
            if (is.isNegated()) {
                List<ConditionExpression> noperands = new ArrayList<ConditionExpression>(1);
                noperands.add(cond);
                cond = new LogicalFunctionCondition("not", noperands,
                                                    is.getType(), is);
            }
            conditions.add(cond);
        }

        protected void addLogicalFunctionCondition(List<ConditionExpression> conditions, 
                                                   ValueNode condition) 
                throws StandardException {
            List<ConditionExpression> operands = new ArrayList<ConditionExpression>();
            String functionName;
            if (condition instanceof UnaryLogicalOperatorNode) {
                addCondition(operands, ((UnaryLogicalOperatorNode)condition).getOperand());
                switch (condition.getNodeType()) {
                case NodeTypes.NOT_NODE:
                    functionName = "not";
                    break;
                default:
                    throw new UnsupportedSQLException("Unsuported condition", condition);
                }
            }
            else if (condition instanceof BinaryLogicalOperatorNode) {
                ValueNode leftOperand = ((BinaryLogicalOperatorNode)
                                         condition).getLeftOperand();
                ValueNode rightOperand = ((BinaryLogicalOperatorNode)
                                          condition).getRightOperand();
                switch (condition.getNodeType()) {
                case NodeTypes.OR_NODE:
                    if (rightOperand.isBooleanFalse()) {
                        addCondition(conditions, leftOperand);
                        return;
                    }
                    functionName = "or";
                    break;
                case NodeTypes.AND_NODE:
                    if (rightOperand.isBooleanTrue()) {
                        addCondition(conditions, leftOperand);
                        return;
                    }
                    functionName = "and";
                    break;
                default:
                    throw new UnsupportedSQLException("Unsuported condition", condition);
                }
                addCondition(operands, leftOperand);
                addCondition(operands, rightOperand);
            }
            else
                throw new UnsupportedSQLException("Unsuported condition", condition);
            conditions.add(new LogicalFunctionCondition(functionName, operands,
                                                        condition.getType(), condition));
        }

        /** Is this a boolean condition used as a normal value? */
        protected boolean isConditionExpression(ValueNode value)
                throws StandardException {
            switch (value.getNodeType()) {
            case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
            case NodeTypes.BETWEEN_OPERATOR_NODE:
            case NodeTypes.IN_LIST_OPERATOR_NODE:
            case NodeTypes.LIKE_OPERATOR_NODE:
            case NodeTypes.IS_NULL_NODE:
            case NodeTypes.IS_NOT_NULL_NODE:
            case NodeTypes.IS_NODE:
            case NodeTypes.OR_NODE:
            case NodeTypes.AND_NODE:
            case NodeTypes.NOT_NODE:
                return true;
            case NodeTypes.SUBQUERY_NODE:
                return (((SubqueryNode)value).getSubqueryType() != 
                        SubqueryNode.SubqueryType.EXPRESSION);
            default:
                return false;
            }
        }

        /** Get given condition as a single node. */
        protected ConditionExpression toCondition(ValueNode condition)
                throws StandardException {
            List<ConditionExpression> conditions = new ArrayList<ConditionExpression>(1);
            addCondition(conditions, condition);
            if (conditions.size() == 1)
                return conditions.get(0);
            // CASE WHEN x BETWEEN a AND b means multiple conditions from single one in AST.
            else
                return new LogicalFunctionCondition("and", conditions,
                                                    condition.getType(), condition);
        }

        /** SELECT DISTINCT with sorting sorts by an input Project and
         * adds extra columns so as to only sort once for both
         * Distinct and the requested ordering.
         */
        protected List<OrderByExpression> sortsForDistinct(List<OrderByExpression> sorts,
                                                           Project project)
                throws StandardException {
            List<ExpressionNode> exprs = project.getFields();
            BitSet used = new BitSet(exprs.size());
            for (OrderByExpression orderBy : sorts) {
                ExpressionNode expr = orderBy.getExpression();
                int idx = exprs.indexOf(expr);
                if (idx < 0)
                    throw new UnsupportedSQLException("SELECT DISTINCT requires that ORDER BY expressions be in the select list",
                                                      expr.getSQLsource());
                ExpressionNode cexpr = new ColumnExpression(project, idx,
                                                            expr.getSQLtype(),
                                                            expr.getSQLsource());
                orderBy.setExpression(cexpr);
                used.set(idx);
            }
            for (int i = 0; i < exprs.size(); i++) {
                if (!used.get(i)) {
                    ExpressionNode expr = exprs.get(i);
                    ExpressionNode cexpr = new ColumnExpression(project, i,
                                                                expr.getSQLtype(),
                                                                expr.getSQLsource());
                    OrderByExpression orderBy = new OrderByExpression(cexpr,
                                                                      sorts.get(0).isAscending());
                    sorts.add(orderBy);
                }
            }
            return sorts;
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
                    limit = getIntegerConstant(limitClause, 
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
            return toExpression(valueNode, null);
        }

        protected ExpressionNode toExpression(ValueNode valueNode,
                                              List<ExpressionNode> projects)
                throws StandardException {
            if (valueNode == null) {
                return new ConstantExpression(null, AkType.NULL);
            }
            DataTypeDescriptor type = valueNode.getType();
            if (valueNode instanceof ColumnReference) {
                ColumnBinding cb = (ColumnBinding)((ColumnReference)valueNode).getUserData();
                if (cb == null)
                    throw new UnsupportedSQLException("Unsupported column", valueNode);
                Joinable joinNode = joinNodes.get(cb.getFromTable());
                if ((joinNode == null) &&
                    (cb.getFromTable() == null) &&
                    (projects != null) &&
                    (cb.getResultColumn() != null)) {
                    // Alias: use result column expression.
                    return projects.get(cb.getResultColumn().getColumnPosition()-1);
                }
                if (!(joinNode instanceof ColumnSource))
                    throw new UnsupportedSQLException("Unsupported column", valueNode);
                Column column = cb.getColumn();
                if (column != null)
                    return new ColumnExpression(((TableSource)joinNode), column, 
                                                type, valueNode);
                else
                    return new ColumnExpression(((ColumnSource)joinNode), 
                                                cb.getFromTable().getResultColumns().indexOf(cb.getResultColumn()), 
                                                type, valueNode);
            }
            else if (valueNode instanceof ConstantNode) {
                if (valueNode instanceof BooleanConstantNode)
                    return new BooleanConstantExpression((Boolean)((ConstantNode)valueNode).getValue(), 
                                                         type, valueNode);
                else
                    return new ConstantExpression(((ConstantNode)valueNode).getValue(), 
                                                  type, valueNode);
            }
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
            else if (isConditionExpression(valueNode)) {
                return toCondition(valueNode);
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
                int nodeType = valueNode.getNodeType();
                switch (nodeType) {
                case NodeTypes.CONCATENATION_OPERATOR_NODE:
                    // Operator is binary but function is nary: collapse.
                    while (true) {
                        operands.add(toExpression(binary.getLeftOperand()));
                        ValueNode right = binary.getRightOperand();
                        if (right.getNodeType() != nodeType) {
                            operands.add(toExpression(right));
                            break;
                        }
                        binary = (BinaryOperatorNode)right;
                    }
                    break;
                default:
                    operands.add(toExpression(binary.getLeftOperand()));
                    operands.add(toExpression(binary.getRightOperand()));
                }
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
            else if (valueNode instanceof CoalesceFunctionNode) {
                CoalesceFunctionNode coalesce = (CoalesceFunctionNode)valueNode;
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>();
                for (ValueNode value : coalesce.getArgumentsList()) {
                    operands.add(toExpression(value));
                }
                return new FunctionExpression(coalesce.getFunctionName(),
                                              operands,
                                              coalesce.getType(), coalesce);
            }
            else if (valueNode instanceof SubqueryNode) {
                SubqueryNode subqueryNode = (SubqueryNode)valueNode;
                PlanNode subquery = toQueryForSelect(subqueryNode.getResultSet(),
                                                     subqueryNode.getOrderByList(),
                                                     subqueryNode.getOffset(),
                                                     subqueryNode.getFetchFirst());
                return new SubqueryValueExpression(new Subquery(subquery),
                                                   subqueryNode.getType(), subqueryNode);
            }
            else if (valueNode instanceof JavaToSQLValueNode) {
                return toExpression(((JavaToSQLValueNode)valueNode).getJavaValueNode(),
                                    valueNode,
                                    false);
            }
            else if (valueNode instanceof CurrentDatetimeOperatorNode) {
                String functionName = null;
                switch (((CurrentDatetimeOperatorNode)valueNode).getField()) {
                case DATE:
                    functionName = "current_date";
                    break;
                case TIME:
                    functionName = "current_time";
                    break;
                case TIMESTAMP:
                    functionName = "current_timestamp";
                    break;
                }
                return new FunctionExpression(functionName,
                                              Collections.<ExpressionNode>emptyList(),
                                              valueNode.getType(), valueNode);
            }
            else if (valueNode instanceof SpecialFunctionNode) {
                String functionName = null;
                switch (valueNode.getNodeType()) {
                case NodeTypes.USER_NODE:
                case NodeTypes.CURRENT_USER_NODE:
                    functionName = "current_user";
                    break;
                case NodeTypes.SESSION_USER_NODE:
                    functionName = "session_user";
                    break;
                case NodeTypes.SYSTEM_USER_NODE:
                    functionName = "system_user";
                    break;
                case NodeTypes.CURRENT_ISOLATION_NODE:
                case NodeTypes.IDENTITY_VAL_NODE:
                case NodeTypes.CURRENT_SCHEMA_NODE:
                case NodeTypes.CURRENT_ROLE_NODE:
                default:
                    throw new UnsupportedSQLException("Unsupported special function", valueNode);
                }
                return new FunctionExpression(functionName,
                                              Collections.<ExpressionNode>emptyList(),
                                              valueNode.getType(), valueNode);
            }
            else if (valueNode instanceof ConditionalNode) {
                ConditionalNode cond = (ConditionalNode)valueNode;
                return new IfElseExpression(toConditions(cond.getTestCondition()),
                                            toExpression(cond.getThenNode()),
                                            toExpression(cond.getElseNode()),
                                            cond.getType(), cond);
            }
            else
                throw new UnsupportedSQLException("Unsupported operand", valueNode);
        }

        // TODO: Need to figure out return type.  Maybe better to have
        // done this earlier and bound to a known function and elided the
        // Java stuff then.
        protected ExpressionNode toExpression(JavaValueNode javaToSQL,
                                              ValueNode valueNode,
                                              boolean asCondition)
                throws StandardException {
            if (javaToSQL instanceof MethodCallNode) {
                MethodCallNode methodCall = (MethodCallNode)javaToSQL;
                List<ExpressionNode> operands = new ArrayList<ExpressionNode>();
                if (methodCall.getMethodParameters() != null) {
                    for (JavaValueNode javaValue : methodCall.getMethodParameters()) {
                        operands.add(toExpression(javaValue, null, false));
                    }
                }
                if (asCondition)
                    return new FunctionCondition(methodCall.getMethodName(),
                                                 operands,
                                                 valueNode.getType(), valueNode);
                else
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

        /** Value for ORDER / GROUP BY.
         * Can be:<ul>
         * <li>ordinary expression</li>
         * <li>ordinal index into the projects</li>
         * <li>alias of one of the projects</li></ul>
         */
        protected ExpressionNode toOrderGroupBy(ValueNode valueNode,
                                                List<ExpressionNode> projects,
                                                String which)
                throws StandardException {
            ExpressionNode expression = toExpression(valueNode, projects);
            if (expression.isConstant()) {
                Object value = ((ConstantExpression)expression).getValue();
                if (value instanceof Long) {
                    int i = ((Long)value).intValue();
                    if ((i <= 0) || (i > projects.size()))
                        throw new OrderGroupByIntegerOutOfRange(which, i, projects.size());
                    expression = (ExpressionNode)projects.get(i-1);
                }
                else
                    throw new OrderGroupByNonIntegerConstant(which, expression.getSQLsource());
            }
            return expression;
        }

        /** Construct an aggregating node.
         * This only sets the skeleton with the group by fields. Later,
         * aggregate functions from the result columns, HAVING & ORDER BY
         * clauses will be added there and the result column adjusted to
         * reflect this.
         */
        protected AggregateSource toAggregateSource(PlanNode input,
                                                    GroupByList groupByList,
                                                    List<ExpressionNode> projects)
                throws StandardException {
            List<ExpressionNode> groupBy = new ArrayList<ExpressionNode>();
            if (groupByList != null) {
                for (GroupByColumn groupByColumn : groupByList) {
                    groupBy.add(toOrderGroupBy(groupByColumn.getColumnExpression(),
                                               projects, "GROUP"));
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
            return HasAggregateFunction.of(expr);
        }

        public static class HasAggregateFunction implements ExpressionVisitor {
            private boolean found = false;

            @Override
            public boolean visitEnter(ExpressionNode n) {
                return visit(n);
            }
            @Override
            public boolean visitLeave(ExpressionNode n) {
                return !found;
            }
            @Override
            public boolean visit(ExpressionNode n) {
                if (n instanceof AggregateFunctionExpression) {
                    found = true;
                    return false;
                }
                return true;
            }

            static boolean of(ExpressionNode expr) {
                HasAggregateFunction haf = new HasAggregateFunction();
                expr.accept(haf);
                return haf.found;
            }
        }

    }
}
