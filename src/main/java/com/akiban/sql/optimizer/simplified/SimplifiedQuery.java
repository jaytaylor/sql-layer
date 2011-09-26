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

package com.akiban.sql.optimizer.simplified;

import com.akiban.sql.optimizer.*;
import com.akiban.sql.optimizer.plan.TableTreeBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.akiban.server.error.ColumnNotBoundException;
import com.akiban.server.error.UnsupportedSQLException;
import com.akiban.sql.parser.*;

import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

import static com.akiban.qp.expression.API.*;
import static com.akiban.qp.operator.API.JoinType;
import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;

/**
 * An SQL DML statement turned into a simpler form for the interim
 * heuristic optimizer.
 *
 * Takes care of representing what we can optimize today and rejecting
 * what we cannot.
 */
public class SimplifiedQuery
{
    public static class TableTree extends TableTreeBase<TableNode> {
        protected TableNode createNode(UserTable table) {
            return new TableNode(table);
        }

        /** Make the given node and its ancestors the left branch (the
         * one reached by just firstChild() links. */
        public void setLeftBranch(TableNode node) {
            while (true) {
                TableNode parent = node.getParent();
                if (parent == null) break;
                TableNode firstSibling = parent.getFirstChild();
                if (node != firstSibling) {
                    TableNode sibling = firstSibling;
                    while (true) {
                        assert (sibling != null) : "node not in sibling list";
                        TableNode next = sibling.getNextSibling();
                        if (next == node) {
                            // Splice node out.
                            sibling.setNextSibling(node.getNextSibling());
                            break;
                        }
                        sibling = next;
                    }
                    node.setNextSibling(firstSibling);
                    parent.setFirstChild(node);
                }
                node = parent;
            }
        }

        /** Determine branch occurrence.
         * @return the number of branches. */
        public int colorBranches() {
            return colorBranches(root, 0);
        }

        private int colorBranches(TableNode node, int nbranches) {
            int branches = 0;
            for (TableNode child = node.getFirstChild(); 
                 child != null; 
                 child = child.getNextSibling()) {
                nbranches = colorBranches(child, nbranches);
                if (child.isUsed()) {
                    // A parent is on the same branch as any child.
                    branches |= child.getBranches();
                }
            }
            if (branches == 0) {
                if (node.isUsed()) {
                    // The leaf of a new branch.
                    branches = (1 << nbranches++);
                }
            }
            else if (Integer.bitCount(branches) > 1) {
                // A parent of children on different branches must be
                // included to get the necessary cross-product join
                // fields.
                node.setUsed(true);
            }
            node.setBranches(branches);
            return nbranches;
        }
    }

    public static class TableNode extends TableTreeBase.TableNodeBase<TableNode> {
        private boolean used, optional;
        private List<ColumnCondition> conditions;
        private List<SimpleSelectColumn> selectColumns;
        private int branches;

        public TableNode(UserTable table) {
            super(table);
        }

        public boolean isUsed() {
            return used;
        }
        public void setUsed(boolean used) {
            this.used = used;
        }

        /** Is this table on the optional end of an outer join? */
        public boolean isOptional() {
            return optional;
        }
        public boolean isRequired() {
            return !optional;
        }
        public void setOptional(boolean optional) {
            this.optional = optional;
        }

        public boolean hasConditions() {
            return ((conditions != null) && !conditions.isEmpty());
        }

        public List<ColumnCondition> getConditions() {
            return conditions;
        }

        protected void addCondition(ColumnCondition condition) {
            if (conditions == null)
                conditions = new ArrayList<ColumnCondition>();
            conditions.add(condition);
        }

        public boolean hasSelectColumns() {
            return ((selectColumns != null) && !selectColumns.isEmpty());
        }

        public List<SimpleSelectColumn> getSelectColumns() {
            return selectColumns;
        }

        protected void addSelectColumn(SimpleSelectColumn selectColumn) {
            if (selectColumns == null)
                selectColumns = new ArrayList<SimpleSelectColumn>();
            selectColumns.add(selectColumn);
        }

        public int getBranches() {
            return branches;
        }
        public void setBranches(int branches) {
            this.branches = branches;
        }

        public boolean isUsedOnBranch(int branch) {
            return used && ((branches & (1 << branch)) != 0);
        }

        /** Is this table or any beneath it included in the query? */
        public boolean subtreeUsed() {
            for (TableNode descendant : subtree())
                if (descendant.isUsed())
                    return true;
            return false;
        }
    }

    public static abstract class BaseJoinNode {
        // For now, order by tableId, i.e. creation time.
        public abstract Integer getMinOrdinal();
        public abstract Integer getMaxOrdinal();

        public boolean isTable() {
            return false;
        }
        public boolean isInnerJoin() {
            return false;
        }

        // Return true if conditions mean this node cannot be left
        // out, after adjusting for such inputs.
        public abstract boolean promoteOuterJoins(Collection<TableNode> conditionTables);
        public abstract void promotedOuterJoin();
    }

    // A join to an actual table.
    public static class TableJoinNode extends BaseJoinNode {
        private TableNode table;

        public TableJoinNode(TableNode table) {
            this.table = table;
        }

        public TableNode getTable() {
            return table;
        }

        public Integer getMinOrdinal() {
            return table.getOrdinal();
        }
        public Integer getMaxOrdinal() {
            return table.getOrdinal();
        }

        public boolean isTable() {
            return true;
        }

        public boolean promoteOuterJoins(Collection<TableNode> conditionTables) {
            return conditionTables.contains(table);
        }

        public void promotedOuterJoin() {
            table.setOptional(false);
        }

        public String toString() {
            return table.toString();
        }
    }

    // A join between two tables / subjoins.
    public static class JoinJoinNode extends BaseJoinNode {
        private BaseJoinNode left, right;
        private JoinType joinType;
        private Integer minOrdinal, maxOrdinal;

        public JoinJoinNode(BaseJoinNode left, BaseJoinNode right,
                            JoinType joinType) {
            this.left = left;
            this.right = right;
            this.joinType = joinType;
        }

        public BaseJoinNode getLeft() {
            return left;
        }
        public void setLeft(BaseJoinNode left) {
            this.left = left;
        }
        public BaseJoinNode getRight() {
            return right;
        }
        public void setRight(BaseJoinNode right) {
            this.right = right;
        }

        public Integer getMinOrdinal() {
            if (minOrdinal == null) {
                minOrdinal = left.getMinOrdinal();
                if (minOrdinal > right.getMinOrdinal())
                    minOrdinal = right.getMinOrdinal();
            }
            return minOrdinal;
        }
        public Integer getMaxOrdinal() {
            if (maxOrdinal == null) {
                maxOrdinal = left.getMaxOrdinal();
                if (maxOrdinal < right.getMaxOrdinal())
                    maxOrdinal = right.getMaxOrdinal();
            }
            return maxOrdinal;
        }

        public JoinType getJoinType() {
            return joinType;
        }

        public boolean isInnerJoin() {
            return (joinType == JoinType.INNER_JOIN);
        }

        // If the optional side of an outer join cannot be null, turn it into inner.
        // If either side of a join has a condition, then the join result does.
        public boolean promoteOuterJoins(Collection<TableNode> conditionTables) {
            boolean lp = left.promoteOuterJoins(conditionTables);
            boolean rp = right.promoteOuterJoins(conditionTables);
            boolean promoted = false;
            switch (joinType) {
            case LEFT_JOIN:
                promoted = rp;
                break;
            case RIGHT_JOIN:
                promoted = lp;
                break;
            }
            if (promoted) {
                joinType = JoinType.INNER_JOIN;
                promotedOuterJoin();
            }
            return lp || rp;
        }

        public void promotedOuterJoin() {
            if (joinType == JoinType.INNER_JOIN) {
                left.promotedOuterJoin();
                right.promotedOuterJoin();
            }
        }

        // Reverse operands and outer join direction if necessary.
        public void reverse() {
            BaseJoinNode temp = left;
            left = right;
            right = temp;
            switch (joinType) {
            case LEFT_JOIN:
                joinType = JoinType.RIGHT_JOIN;
                break;
            case RIGHT_JOIN:
                joinType = JoinType.LEFT_JOIN;
                break;
            }
        }

        public String toString() {
            return joinType + "(" + left + "," + right + ")";
        }
    }

    // An expression used as select column.
    public static class SimpleSelectColumn {
        private String name;
        private boolean nameDefaulted;
        private SimpleExpression expression;
        private DataTypeDescriptor type;

        public SimpleSelectColumn(String name, boolean nameDefaulted,
                                  SimpleExpression expression,
                                  DataTypeDescriptor type) {
            this.name = name;
            this.nameDefaulted = nameDefaulted;
            this.expression = expression;
            this.type = type;
        }

        public String getName() {
            return name;
        }
        public boolean isNameDefaulted() {
            return nameDefaulted;
        }

        public SimpleExpression getExpression() {
            return expression;
        }

        public DataTypeDescriptor getType() {
            return type;
        }
    }

    public interface ColumnExpressionToIndex {
        public int getIndex(ColumnExpression column);
    }

    public static class TableNodeOffsets implements ColumnExpressionToIndex {
        private Map<TableNode,Integer> tableOffsets;

        public TableNodeOffsets(Map<TableNode,Integer> tableOffsets) {
            this.tableOffsets = tableOffsets;
        }

        public int getIndex(ColumnExpression column) {
            Integer offset = tableOffsets.get(column.getTable());
            assert offset != null;
            return offset + column.getColumn().getPosition();
        }
    }

    public static class ColumnIndexMap implements ColumnExpressionToIndex {
        private Map<Column,Integer> columnOffsets;

        public ColumnIndexMap(Map<Column,Integer> columnOffsets) {
            this.columnOffsets = columnOffsets;
        }

        public int getIndex(ColumnExpression column) {
            Integer offset = columnOffsets.get(column.getColumn());
            assert offset != null;
            return offset;
        }
    }

    // One of the operands to a boolean equality / inequality
    // condition, the only kind currently supported.
    public static abstract class SimpleExpression {
        public boolean isColumn() {
            return false;
        }
        public abstract Expression generateExpression(ColumnExpressionToIndex fieldOffsets);
    }

    // An operand from a table column.
    public static class ColumnExpression extends SimpleExpression {
        private TableNode table;
        private Column column;

        public ColumnExpression(TableNode table, Column column) {
            this.table = table;
            this.column = column;
        }

        public TableNode getTable() {
            return table;
        }

        public Column getColumn() {
            return column;
        }

        public String toString() {
            return column.toString();
        }

        public boolean isColumn() {
            return true;
        }

        public Expression generateExpression(ColumnExpressionToIndex fieldOffsets)  {
            return field(fieldOffsets.getIndex(this));
        }
    }

    // An operand with a constant literal value.
    public static class LiteralExpression extends SimpleExpression {
        private Object value;

        public LiteralExpression(Object value) {
            if (value instanceof Integer)
                value = new Long(((Integer)value).intValue());
            this.value = value;
        }

        public Object getValue() {
            return value;
        }

        public String toString() {
            return value.toString();
        }

        public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
            return literal(value);
        }
    }

    // An operand with a parameter value.
    public static class ParameterExpression extends SimpleExpression {
        private int position;

        public ParameterExpression(int position) {
            this.position = position;
        }

        public int getPosition() {
            return position;
        }

        public String toString() {
            return "$" + position;
        }

        public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
            return variable(position);
        }
    }

    // The aggregate COUNT(*)
    public static class CountStarExpression extends SimpleExpression {
        public CountStarExpression() {
        }

        public String toString() {
            return "COUNT(*)";
        }

        public Expression generateExpression(ColumnExpressionToIndex fieldOffsets)  {
            throw new UnsupportedSQLException("COUNT(*)", null);
        }
    }

    // A boolean condition between a column and something else that
    // can be turned into an expression.
    public static class ColumnCondition {
        private ColumnExpression left;
        private SimpleExpression right;
        private Comparison operation;
        private ColumnCondition mirror;

        public ColumnCondition(ColumnExpression left, SimpleExpression right,
                               Comparison operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;

            getTable().addCondition(this);
        }

        public TableNode getTable() {
            return left.getTable();
        }

        public ColumnExpression getLeft() {
            return left;
        }
        public SimpleExpression getRight() {
            return right;
        }

        public Comparison getOperation() {
            return operation;
        }

        public boolean isConstant() {
            return !right.isColumn();
        }

        public boolean isSingleTable() {
            if (!right.isColumn())
                return true;
            else
                return (left.getTable() == ((ColumnExpression)right).getTable());
        }

        // Does this condition match the given column?
        // Handles the case where it matches the RHS by returning a mirror image.
        public ColumnCondition matches(Column column) {
            if (column == left.getColumn())
                return this;
            if (right.isColumn() &&
                (column == ((ColumnExpression)right).getColumn()))
                return getMirror();
            return null;
        }

        // Is this a condition between the given column and a constant
        // of the given comparison type?
        public boolean isColumnConstantCondition(Column column,
                                                 Comparison comparison) {
            if ((column != left.getColumn()) || !isConstant())
                return false;
            switch (comparison) {
            case EQ:
                return (operation == Comparison.EQ);
            case LT:
                return ((operation == Comparison.LT) || (operation == Comparison.LE));
            case GT:
                return ((operation == Comparison.GT) || (operation == Comparison.GE));
            default:
                return false;
            }
        }

        public static Comparison reverseComparison(Comparison operation) {
            switch (operation) {
            case EQ:
            case NE:
                return operation;
            case LT:
                return Comparison.GT;
            case LE:
                return Comparison.GE;
            case GT:
                return Comparison.LT;
            case GE:
                return Comparison.LE;
            default:
                assert false : operation;
                return null;
            }
        }

        // Get this condition with the operands reversed, so that
        // other column is on the left.
        protected ColumnCondition getMirror() {
            if (mirror == null)
                mirror = new ColumnCondition((ColumnExpression)right,
                                             left, reverseComparison(operation));
            return mirror;
        }

        public String toString() {
            return left + " " + operation + " " + right;
        }

        // Turn this condition into an operator Expression.
        public Expression generateExpression(ColumnExpressionToIndex fieldOffsets) {
            return compare(left.generateExpression(fieldOffsets),
                           operation,
                           right.generateExpression(fieldOffsets));
        }
    }

    // Sorting by a table column, the only case supported.
    public static class SortColumn {
        private Column column;
        private boolean ascending;

        public SortColumn(Column column, boolean ascending) {
            this.column = column;
            this.ascending = ascending;
        }

        public Column getColumn() {
            return column;
        }
        public boolean isAscending() {
            return ascending;
        }

        public String toString() {
            if (ascending)
                return column.toString();
            else
                return column.toString() + " DESC";
        }
    }

    private GroupBinding group = null;
    private BaseJoinNode joins = null;
    private List<List<SimpleExpression>> values = null;
    private TableTree tables = new TableTree();
    private List<SimpleSelectColumn> selectColumns = null;
    private List<ColumnCondition> conditions = new ArrayList<ColumnCondition>();
    private List<SortColumn> sortColumns = null;
    private int offset = 0;
    private int limit = -1;
    private List<Set<Column>> columnEquivalences = new ArrayList<Set<Column>>();
    private Map<Column,Set<Column>> columnEquivalencesByColumn = new HashMap<Column,Set<Column>>();

    public SimplifiedQuery(DMLStatementNode statement, Set<ValueNode> joinConditions) {
        fillFromResultSet(statement.getResultSetNode(), joinConditions);
    }

    protected void fillFromResultSet(ResultSetNode resultSet,
                                     Set<ValueNode> joinConditions) {
        if (resultSet instanceof SelectNode) {
            fillFromSelect((SelectNode)resultSet, joinConditions);
        }
        else {
            values = new ArrayList<List<SimpleExpression>>();
            fillFromValues(resultSet);
        }
    }

    protected final void fillFromValues(ResultSetNode resultSet) {
        if (resultSet instanceof RowResultSetNode) {
            ResultColumnList resultColumns = resultSet.getResultColumns();
            List<SimpleExpression> row = new ArrayList<SimpleExpression>(resultColumns.size());
            for (ResultColumn resultColumn : resultColumns) {
                row.add(getSimpleExpression(resultColumn.getExpression()));
            }
            values.add(row);
        }
        else if (resultSet instanceof UnionNode) {
            UnionNode valuesList = (UnionNode)resultSet;
            fillFromValues(valuesList.getLeftResultSet());
            fillFromValues(valuesList.getRightResultSet());
        }
        else
            throw new UnsupportedSQLException("Unsupported result set", resultSet);
    }

    protected void fillFromSelect(SelectNode select, Set<ValueNode> joinConditions) {
        if (select.getGroupByList() != null)
            throw new UnsupportedSQLException("GROUP BY", select);
        if (select.isDistinct())
            throw new UnsupportedSQLException("DISTINCT", select);
        if (select.hasWindows())
            throw new UnsupportedSQLException("WINDOW", select);

        for (FromTable fromTable : select.getFromList()) {
            if (joins == null)
                joins = getJoinNode(fromTable, false);
            else
                joins = joinNodes(joins, getJoinNode(fromTable, false), JoinType.INNER_JOIN);
        }

        ValueNode whereClause = select.getWhereClause();
        while (whereClause != null) {
            if (whereClause.isBooleanTrue()) break;
            if (!(whereClause instanceof AndNode))
                throw new UnsupportedSQLException("Unsupported complex WHERE",
                                                  whereClause);
            AndNode andNode = (AndNode)whereClause;
            whereClause = andNode.getRightOperand();
            ValueNode condition = andNode.getLeftOperand();
            if (joinConditions.contains(condition))
                continue;
            addCondition(condition);
        }

        ResultColumnList rcl = select.getResultColumns();
        if (rcl != null) {
            selectColumns = new ArrayList<SimpleSelectColumn>(rcl.size());
            for (ResultColumn result : select.getResultColumns()) {
                SimpleExpression expr = getSimpleExpression(result.getExpression());
                String name = result.getName();
                boolean nameDefaulted =
                    // TODO: Maybe mark this in the grammar. Or don't
                    // worry and accept case imposed by parser.
                    (result.getExpression() instanceof ColumnReference) &&
                    (name == ((ColumnReference)result.getExpression()).getColumnName());
                SimpleSelectColumn column = new SimpleSelectColumn(name, nameDefaulted,
                                                                   expr,
                                                                   result.getType());
                selectColumns.add(column);
                if (expr.isColumn()) {
                    ColumnExpression columnExpression = (ColumnExpression)expr;
                    columnExpression.getTable().addSelectColumn(column);
                }
            }
        }

        for (ValueNode joinCondition : joinConditions) {
            BinaryRelationalOperatorNode binop = (BinaryRelationalOperatorNode)
                joinCondition;
            Column left = getColumnReferenceColumn(binop.getLeftOperand(), null);
            Column right = getColumnReferenceColumn(binop.getRightOperand(), null);
            if ((left != null) && (right != null))
                addColumnEquivalence(left, right);
        }
        for (ColumnCondition whereCondition : conditions) {
            if ((whereCondition.getOperation() == Comparison.EQ) &&
                whereCondition.getRight().isColumn())
                addColumnEquivalence(whereCondition.getLeft().getColumn(),
                                     ((ColumnExpression)
                                      whereCondition.getRight()).getColumn());
        }
    }

    protected void addCondition(ValueNode condition) {
        switch (condition.getNodeType()) {
        case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.EQ);
            break;
        case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.GT);
            break;
        case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.GE);
            break;
        case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.LT);
            break;
        case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.LE);
            break;
        case NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE:
            addBinaryCondition((BinaryOperatorNode)condition, Comparison.NE);
            break;
        case NodeTypes.BETWEEN_OPERATOR_NODE:
            addBetweenCondition((BetweenOperatorNode)condition);
            break;
        case NodeTypes.IN_LIST_OPERATOR_NODE:
            addInCondition((InListOperatorNode)condition);
            break;
        case NodeTypes.BOOLEAN_CONSTANT_NODE:
            if (condition.isBooleanTrue())
                break;
            /* else falls through */
        default:
            throw new UnsupportedSQLException("Unsupported WHERE predicate",
                                              condition);
        }
    }

    protected void addBinaryCondition(BinaryOperatorNode binop, Comparison op) {
        SimpleExpression left, right;
        left = getSimpleExpression(binop.getLeftOperand());
        right = getSimpleExpression(binop.getRightOperand());
        if (!left.isColumn()) {
            if (right.isColumn()) {
                SimpleExpression temp = left;
                left = right;
                right = temp;
                op = ColumnCondition.reverseComparison(op);
            }
            else if ((left instanceof LiteralExpression) && 
                     (right instanceof LiteralExpression)) {
                // Know that literals don't need offsets, rows or bindings.
                boolean answer = Extractors.getBooleanExtractor().getBoolean(
                    compare(left.generateExpression(null), 
                            op,
                            right.generateExpression(null)).get().evaluation().eval()
                    );
                if (answer) return; // Boolean true: nothing to add.
                // Boolean false; no way to add such a condition; throw error.
            }
            throw new UnsupportedSQLException("WHERE operands", binop);
        }
        conditions.add(new ColumnCondition((ColumnExpression)left, right, op));
    }

    protected void addBetweenCondition(BetweenOperatorNode between) {
        ColumnExpression left = getColumnExpression(between.getLeftOperand());
        if (left == null)
            throw new UnsupportedSQLException("BETWEEN operand",
                                              between.getLeftOperand());
        ValueNodeList rightOperandList = between.getRightOperandList();
        SimpleExpression right1 = getSimpleExpression(rightOperandList.get(0));
        SimpleExpression right2 = getSimpleExpression(rightOperandList.get(1));
        conditions.add(new ColumnCondition(left, right1, Comparison.GE));
        conditions.add(new ColumnCondition(left, right2, Comparison.LE));
    }

    protected void addInCondition(InListOperatorNode in) {
        ColumnExpression left = getColumnExpression(in.getLeftOperand());
        if (left == null)
            throw new UnsupportedSQLException("IN operand",
                                              in.getLeftOperand());
        ValueNodeList rightOperandList = in.getRightOperandList();
        if (rightOperandList.size() != 1)
            throw new UnsupportedSQLException("IN predicate", in);
        SimpleExpression right1 = getSimpleExpression(rightOperandList.get(0));
        conditions.add(new ColumnCondition(left, right1, Comparison.EQ));
    }

    protected void fillFromOrderBy(OrderByList orderByList) {
        List<SortColumn> sc = new ArrayList<SortColumn>();
        for (OrderByColumn orderByColumn : orderByList) {
            Column column = getColumnReferenceColumn(orderByColumn.getExpression(),
                                                     "ORDER BY column");
            // If column has a constant value, there is no need to sort on it.
            if (!isColumnConstant(column))
                sc.add(new SortColumn(column, orderByColumn.isAscending()));
        }
        if (!sc.isEmpty())
            sortColumns = sc;
    }

    protected void fillOffset(ValueNode offsetClause) {
        offset = getIntegerConstant(offsetClause, "Unsupported OFFSET");
    }

    protected void fillLimit(ValueNode limitClause) {
        limit = getIntegerConstant(limitClause, "Unsupported LIMIT");
    }

    protected BaseJoinNode getJoinNode(FromTable fromTable, boolean optional) {
        if (fromTable instanceof FromBaseTable) {
            TableBinding tb = (TableBinding)fromTable.getUserData();
            if (tb == null)
                throw new UnsupportedSQLException("FROM table",
                                                  fromTable);
            GroupBinding gb = tb.getGroupBinding();
            if (gb == null)
                throw new UnsupportedSQLException("FROM non-group",
                                                  fromTable);
            if (group == null)
                group = gb;
            else if (group != gb)
                throw new UnsupportedSQLException("multiple group joins", fromTable);
            TableNode table = tables.addNode((UserTable)tb.getTable());
            if (table.isUsed())
                throw new UnsupportedSQLException("self join", fromTable);
            table.setUsed(true);
            table.setOptional(optional);
            return new TableJoinNode(table);
        }
        else if (fromTable instanceof JoinNode) {
            JoinNode joinNode = (JoinNode)fromTable;
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
            return joinNodes(getJoinNode((FromTable)joinNode.getLeftResultSet(),
                                         optional || ((joinType == JoinType.RIGHT_JOIN) ||
                                                   (joinType == JoinType.FULL_JOIN))),
                             getJoinNode((FromTable)joinNode.getRightResultSet(),
                                         optional || ((joinType == JoinType.LEFT_JOIN) ||
                                                   (joinType == JoinType.FULL_JOIN))),
                             joinType);
        }
        else if (fromTable instanceof CurrentOfNode) {
            throw new UnsupportedSQLException("Unsupported WHERE CURRENT OF", fromTable);
        }
        else
            throw new UnsupportedSQLException("Unsupported FROM non-table", fromTable);
    }

    protected BaseJoinNode joinNodes(BaseJoinNode left, BaseJoinNode right,
                                     JoinType joinType) {
        return new JoinJoinNode(left, right, joinType);
    }

    public ColumnExpression getColumnExpression(Column column) {
        TableNode table = getColumnTable(column);
        return new ColumnExpression(table, column);
    }

    protected ColumnExpression getColumnExpression(ValueNode operand) {
        Column column = getColumnReferenceColumn(operand, null);
        if (column == null) return null;
        return getColumnExpression(column);
    }

    protected SimpleExpression getSimpleExpression(ValueNode operand) {
        if (operand instanceof ColumnReference)
            return getColumnExpression(getColumnReferenceColumn(operand,
                                                                "Unsupported operand"));
        else if (operand instanceof ConstantNode)
            return new LiteralExpression(((ConstantNode)operand).getValue());
        else if (operand instanceof ParameterNode)
            return new ParameterExpression(((ParameterNode)operand).getParameterNumber());
        else if (operand instanceof CastNode) {
            CastNode castNode = (CastNode)operand;
            operand = castNode.getCastOperand();
            if (operand instanceof ConstantNode)
                return getTypedLiteral(((ConstantNode)operand).getValue(),
                                       castNode.getType());
            else
                return getSimpleExpression(operand);
        }
        else if ((operand instanceof AggregateNode) &&
                 "COUNT(*)".equals(((AggregateNode)operand).getAggregateName()))
            return new CountStarExpression();
        else
            throw new UnsupportedSQLException("Unsupported operand", operand);
    }

    protected SimpleExpression getTypedLiteral(Object object, DataTypeDescriptor type) {
        // TODO: Need some more conversions for other types.
        if (object instanceof String) {
            LongExtractor extractor = null;
            TypeId typeId = type.getTypeId();
            switch (typeId.getTypeFormatId()) {
            case TypeId.FormatIds.DATE_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.DATE);
                break;
            case TypeId.FormatIds.INT_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.INT);
                break;
            case TypeId.FormatIds.LONGINT_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.INT);
                break;
            case TypeId.FormatIds.SMALLINT_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.INT);
                break;
            case TypeId.FormatIds.TIME_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.TIME);
                break;
            case TypeId.FormatIds.TIMESTAMP_TYPE_ID:
                extractor = Extractors.getLongExtractor(AkType.TIMESTAMP);
                break;
            case TypeId.FormatIds.USERDEFINED_TYPE_ID:
                {
                    String name = typeId.getSQLTypeName();
                    if ("datetime".equals(name)) {
                        extractor = Extractors.getLongExtractor(AkType.DATETIME);
                    }
                    else if ("year".equals(name)) {
                        extractor = Extractors.getLongExtractor(AkType.YEAR);
                    }
                }
                break;
            }
            if (extractor != null)
                object = extractor.getLong((String)object);
        }
        return new LiteralExpression(object);
    }

    // Get the column that this node references or else return null or throw given error.
    protected Column getColumnReferenceColumn(ValueNode value, String errmsg) {
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
        throw new ColumnNotBoundException (value.getColumnName(), errmsg);
    }

    // Get the constant integer value that this node represents or else throw error.
    protected int getIntegerConstant(ValueNode value, String errmsg) {
        if (value instanceof NumericConstantNode) {
            Object number = ((NumericConstantNode)value).getValue();
            if (number instanceof Integer)
                return ((Integer)number).intValue();
        }
        throw new UnsupportedSQLException(errmsg, value);
    }

    public GroupBinding getGroup() {
        return group;
    }
    public BaseJoinNode getJoins() {
        return joins;
    }
    public List<List<SimpleExpression>> getValues() {
        return values;
    }
    public TableTree getTables() {
        return tables;
    }
    public List<SimpleSelectColumn> getSelectColumns() {
        return selectColumns;
    }
    public List<ColumnCondition> getConditions() {
        return conditions;
    }
    public List<SortColumn> getSortColumns() {
        return sortColumns;
    }
    public int getOffset() {
        return offset;
    }
    public int getLimit() {
        return limit;
    }

    public TableNode getColumnTable(Column column) {
        return tables.getNode(column.getUserTable());
    }

    public List<Set<Column>> getColumnEquivalences() {
        return columnEquivalences;
    }

    public Set<Column> getColumnEquivalences(Column column) {
        return columnEquivalencesByColumn.get(column);
    }

    // Add an equivalence between two columns.
    // With these, a table can be left out entirely if all its
    // selected / tested columns are equated with ones that come from
    // one that's needed anyway, like from the index.
    protected void addColumnEquivalence(Column c1, Column c2) {
        Set<Column> e1 = columnEquivalencesByColumn.get(c1);
        Set<Column> e2 = columnEquivalencesByColumn.get(c2);
        if (e1 == null) {
            if (e2 == null) {
                // Brand new.
                Set<Column> n = new HashSet<Column>(2);
                n.add(c1);
                n.add(c2);
                columnEquivalences.add(n);
                columnEquivalencesByColumn.put(c1, n);
                columnEquivalencesByColumn.put(c2, n);
            }
            else {
                columnEquivalencesByColumn.put(c1, e2);
            }
        }
        else if (e2 == null) {
            columnEquivalencesByColumn.put(c2, e1);
        }
        else {
            // Have two that need to be merged.
            columnEquivalences.remove(e2);
            for (Column o2 : e2) {
                columnEquivalencesByColumn.put(o2, e1);
            }
        }
    }

    // The initial join tree is in syntax order.
    // Convert it to the preferred AIS order: ancestors on the left as
    // much as possible given that half outer joins are not associative.
    public void reorderJoins() {
        promoteImpossibleOuterJoins();
        joins = reorderJoinNode(joins);
    }

    // If a join is specified as outer, but there is a boolean
    // condition on an attribute of the nullable table, then its
    // operand won't actually be null in any matching row and the join
    // is equivalent to the corresponding inner join. Converting it
    // allows more reordering.
    // Such outer joins usually arise from programmatically generated
    // queries, such as views.
    public void promoteImpossibleOuterJoins() {
        Collection<TableNode> conditionTables = new HashSet<TableNode>();
        for (ColumnCondition condition : conditions) {
            conditionTables.add(condition.getTable());
            if (condition.getRight().isColumn())
                conditionTables.add(((ColumnExpression)
                                     condition.getRight()).getTable());
        }
        joins.promoteOuterJoins(conditionTables);
    }

    // Return size of directly-reachable subtree of all inner joins.
    protected int countInnerJoins(BaseJoinNode join) {
        if (!join.isInnerJoin())
            return 0;
        return 1 +
            countInnerJoins(((JoinJoinNode)join).getLeft()) +
            countInnerJoins(((JoinJoinNode)join).getRight());
    }

    // Accumulate operands of directly-reachable subtree of inner joins.
    protected void getInnerJoins(BaseJoinNode join, Collection<BaseJoinNode> into) {
        if (!join.isInnerJoin())
            into.add(join);
        else {
            getInnerJoins(((JoinJoinNode)join).getLeft(), into);
            getInnerJoins(((JoinJoinNode)join).getRight(), into);
        }
    }

    // Reorder this join node.
    // A subtree of inner nodes can be completely reordered.
    // An outer join (or a lone inner join) can still commute (changing chirality).
    public BaseJoinNode reorderJoinNode(BaseJoinNode join) {
        if (countInnerJoins(join) > 1) {
            List<BaseJoinNode> joins = new ArrayList<BaseJoinNode>();
            getInnerJoins(join, joins);
            for (int i = 0; i < joins.size(); i++) {
                joins.set(i, reorderJoinNode(joins.get(i)));
            }
            Collections.sort(joins, new Comparator<BaseJoinNode>() {
                                 public int compare(BaseJoinNode j1, BaseJoinNode j2) {
                                     return j1.getMaxOrdinal().compareTo(j2.getMaxOrdinal());
                                 }
                             });
            BaseJoinNode result = joins.get(0);
            for (int i = 1; i < joins.size(); i++) {
                result = new JoinJoinNode(result, joins.get(i), JoinType.INNER_JOIN);
            }
            return result;
        }
        if (!join.isTable()) {
            JoinJoinNode jjoin = (JoinJoinNode)join;
            BaseJoinNode left = jjoin.getLeft();
            left = reorderJoinNode(left);
            jjoin.setLeft(left);
            BaseJoinNode right = jjoin.getRight();
            right = reorderJoinNode(right);
            jjoin.setRight(right);
            if (left.getMaxOrdinal().compareTo(right.getMaxOrdinal()) > 0)
                jjoin.reverse();
        }
        return join;
    }

    // Isolate this join node to a single branch.
    public BaseJoinNode isolateJoinNodeBranch(BaseJoinNode join, int mask) {
        if (join.isTable()) {
            TableJoinNode tjoin = (TableJoinNode)join;
            TableNode table = tjoin.getTable();
            if ((table.getBranches() & mask) != 0)
                return tjoin;
            else
                return null;
        }
        else {
            JoinJoinNode jjoin = (JoinJoinNode)join;
            BaseJoinNode left = jjoin.getLeft();
            left = isolateJoinNodeBranch(left, mask);
            BaseJoinNode right = jjoin.getRight();
            right = isolateJoinNodeBranch(right, mask);
            if (left == null)
                return right;
            else if (right == null)
                return left;
            else
                return new JoinJoinNode(left, right, jjoin.getJoinType());
        }
    }

    // Find a boolean condition of the given [in]equality type between
    // the given column and a constant.
    public ColumnCondition findColumnConstantCondition(Column column, 
                                                       Comparison comparison) {
        for (ColumnCondition columnCondition : conditions) {
            if (columnCondition.isColumnConstantCondition(column, comparison))
                return columnCondition;
        }
        return null;
    }

    // Is this column constant due to equality constraint?
    public boolean isColumnConstant(Column column) {
        return (findColumnConstantCondition(column, Comparison.EQ) != null);
    }

    public void removeConditions(Set<ColumnCondition> conditions) {
        this.conditions.removeAll(conditions);
        for (ColumnCondition condition : conditions)
            condition.getTable().getConditions().remove(condition);
    }

    /** Used flags are initially set based on presence in the original query.  
     * Refine them after conditions have been changed / removed to be
     * just those tables needed in output results.
     * Navigation through intermediate tables is implicit in the group
     * structure.
     * (Not currently used; see IndexUsage replacement.)
     */
    public void recomputeUsed() {
        for (TableNode table : tables) {
            table.setUsed(table.hasSelectColumns() || table.hasConditions());
        }
    }

    /** Coalesce out joins involving tables that aren't used any more.
     * (Not called currently, since usage can be per-branch.)
     */
    public void removeUnusedJoins() {
        joins = removeUnusedJoins(joins);
    }

    protected BaseJoinNode removeUnusedJoins(BaseJoinNode join) {
        if (join.isTable()) {
            TableJoinNode tjoin = (TableJoinNode)join;
            if (tjoin.getTable().isUsed())
                return join;
            else
                return null;
        }
        else {
            JoinJoinNode jjoin = (JoinJoinNode)join;
            BaseJoinNode left = removeUnusedJoins(jjoin.getLeft());
            BaseJoinNode right = removeUnusedJoins(jjoin.getRight());
            if (left != null) {
                if (right != null) {
                    jjoin.setLeft(left);
                    jjoin.setRight(right);
                    return join;
                }
                else
                    return left;
            }
            else if (right != null) {
                return right;
            }
            else
                return null;
        }
    }

}
