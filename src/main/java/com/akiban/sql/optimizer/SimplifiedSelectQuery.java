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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

import static com.akiban.qp.expression.API.*;
import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;

import java.util.*;

/**
 * An SQL SELECT statement turned into a simpler form for the interim
 * heuristic optimizer.
 * 
 * Takes care of representing what we can optimize today and rejecting
 * what we cannot.
 */
public class SimplifiedSelectQuery
{
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
        public abstract boolean promoteOuterJoins(Set<UserTable> conditionTables);
    }

    // A join to an actual table.
    public static class TableJoinNode extends BaseJoinNode {
        private UserTable table;

        public TableJoinNode(UserTable table) {
            this.table = table;
        }

        public UserTable getTable() {
            return table;
        }

        public Integer getMinOrdinal() {
            return table.getTableId();
        }
        public Integer getMaxOrdinal() {
            return table.getTableId();
        }

        public boolean isTable() {
            return true;
        }

        public boolean promoteOuterJoins(Set<UserTable> conditionTables) {
            return conditionTables.contains(table);
        }

        public String toString() {
            return table.toString();
        }
    }

    // A kind of join.
    public static enum JoinType {
        INNER, LEFT, RIGHT, FULL
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
            return (joinType == JoinType.INNER);
        }

        // If the optional side of an outer join cannot be null, turn it into inner.
        // If either side of a join has a condition, then the join result does.
        public boolean promoteOuterJoins(Set<UserTable> conditionTables) {
            boolean lp = left.promoteOuterJoins(conditionTables);
            boolean rp = right.promoteOuterJoins(conditionTables);
            switch (joinType) {
            case LEFT:
                if (rp) joinType = JoinType.INNER;
                break;
            case RIGHT:
                if (lp) joinType = JoinType.INNER;
                break;
            }
            return lp || rp;
        }

        // Reverse operands and outer join direction if necessary.
        public void reverse() {
            BaseJoinNode temp = left;
            left = right;
            right = temp;
            switch (joinType) {
            case LEFT:
                joinType = JoinType.RIGHT;
                break;
            case RIGHT:
                joinType = JoinType.LEFT;
                break;
            }
        }

        public String toString() {
            return joinType + "(" + left + "," + right + ")";
        }
    }

    // A result column (necessarily from an actual table column).
    public static class SelectColumn {
        private Column column;

        public SelectColumn(Column column) {
            this.column = column;
        }

        public Column getColumn() {
            return column;
        }

        public String toString() {
            return column.toString();
        }
    }

    // One of the operands to a boolean equality / inequality
    // condition, the only kind currently supported.
    public static abstract class ConditionOperand {
        public boolean isColumn() {
            return false;
        }
        public abstract Expression generateExpression(Map<UserTable,Integer> fieldOffsets);
    }

    // An operand from a table column.
    public static class ColumnConditionOperand extends ConditionOperand {
        private Column column;
        
        public ColumnConditionOperand(Column column) {
            this.column = column;
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

        public Expression generateExpression(Map<UserTable,Integer> fieldOffsets) {
            UserTable table = column.getUserTable();
            return field(fieldOffsets.get(table) + column.getPosition());
        }
    }
    
    // An operand with a constant literal value.
    public static class LiteralConditionOperand extends ConditionOperand {
        private Object value;
        
        public LiteralConditionOperand(Object value) {
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

        public Expression generateExpression(Map<UserTable,Integer> fieldOffsets) {
            return literal(value);
        }
    }

    // An operand with a parameter value.
    public static class VariableConditionOperand extends ConditionOperand {
        private int position;
        
        public VariableConditionOperand(int position) {
            this.position = position;
        }

        public int getPosition() {
            return position;
        }

        public String toString() {
            return "$" + position;
        }

        public Expression generateExpression(Map<UserTable,Integer> fieldOffsets) {
            return variable(position);
        }
    }

    // A boolean condition between a column and something else that
    // can be turned into an expression.
    public static class ColumnCondition {
        private ColumnConditionOperand left;
        private ConditionOperand right;
        private Comparison operation;
        private ColumnCondition mirror;

        public ColumnCondition(ColumnConditionOperand left, ConditionOperand right,
                               Comparison operation) {
            this.left = left;
            this.right = right;
            this.operation = operation;
        }

        public ColumnConditionOperand getLeft() {
            return left;
        }
        public ConditionOperand getRight() {
            return right;
        }
        public Comparison getOperation() {
            return operation;
        }

        public boolean isConstant() {
            return !right.isColumn();
        }

        // Does this condition match the given column?
        // Handles the case where it matches the RHS by returning a mirror image.
        public ColumnCondition matches(Column column) {
            if (column == left.getColumn())
                return this;
            if (right.isColumn() &&
                (column == ((ColumnConditionOperand)right).getColumn()))
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
                mirror = new ColumnCondition((ColumnConditionOperand)right,
                                             left, reverseComparison(operation));
            return mirror;
        }

        public String toString() {
            return left + " " + operation + " " + right;
        }

        // Turn this condition into an operator Expression.
        public Expression generateExpression(Map<UserTable,Integer> fieldOffsets) {
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
    private Set<UserTable> tables = new HashSet<UserTable>();
    private List<SelectColumn> selectColumns = new ArrayList<SelectColumn>();
    private List<ColumnCondition> conditions = new ArrayList<ColumnCondition>();
    private List<SortColumn> sortColumns = null;
    private int offset = 0;
    private int limit = -1;
    private List<Set<Column>> columnEquivalences = new ArrayList<Set<Column>>();
    private Map<Column,Set<Column>> columnEquivalencesByColumn = new HashMap<Column,Set<Column>>();

    // Turn the given SELECT statement into its simplified form.
    public SimplifiedSelectQuery(CursorNode cursor, Set<ValueNode> joinConditions) 
            throws StandardException {
        SelectNode select = (SelectNode)cursor.getResultSetNode();
        if (select.getGroupByList() != null)
            throw new UnsupportedSQLException("Unsupported GROUP BY");
        if (select.isDistinct())
            throw new UnsupportedSQLException("Unsupported DISTINCT");
        if (select.hasWindows())
            throw new UnsupportedSQLException("Unsupported WINDOW");

        for (FromTable fromTable : select.getFromList()) {
            if (joins == null)
                joins = getJoinNode(fromTable);
            else
                joins = joinNodes(joins, getJoinNode(fromTable), JoinType.INNER);
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
            Comparison op;
            switch (condition.getNodeType()) {
            case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
                op = Comparison.EQ;
                break;
            case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
                op = Comparison.GT;
                break;
            case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
                op = Comparison.GE;
                break;
            case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
                op = Comparison.LT;
                break;
            case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
                op = Comparison.LE;
                break;
            default:
                throw new UnsupportedSQLException("Unsupported WHERE predicate", 
                                                  condition);
            }
            BinaryOperatorNode binop = (BinaryOperatorNode)condition;
            ColumnConditionOperand left;
            ConditionOperand right;
            left = getColumnConditionOperand(binop.getLeftOperand());
            if (left != null) {
                right = getConditionOperand(binop.getRightOperand());
            }
            else {
                left = getColumnConditionOperand(binop.getRightOperand());
                if (left == null)
                    throw new UnsupportedSQLException("Unsupported WHERE operands", 
                                                      condition);
                right = getConditionOperand(binop.getLeftOperand());
                op = ColumnCondition.reverseComparison(op);
            }
            conditions.add(new ColumnCondition(left, right, op));
        }
        
        for (ResultColumn result : select.getResultColumns()) {
            Column column = getColumnReferenceColumn(result.getExpression(),
                                                     "Unsupported result column");
            selectColumns.add(new SelectColumn(column));
        }
        
        if (cursor.getOrderByList() != null) {
            List<SortColumn> sc = new ArrayList<SortColumn>();
            for (OrderByColumn orderByColumn : cursor.getOrderByList()) {
                Column column = getColumnReferenceColumn(orderByColumn.getExpression(), 
                                                         "Unsupported ORDER BY column");
                // If column has a constant value, there is no need to sort on it.
                if (!isColumnConstant(column))
                    sc.add(new SortColumn(column, orderByColumn.isAscending()));
            }
            if (!sc.isEmpty())
                sortColumns = sc;
        }

        if (cursor.getOffsetClause() != null)
            offset = getIntegerConstant(cursor.getOffsetClause(),
                                        "Unsupported OFFSET");
        if (cursor.getFetchFirstClause() != null)
            limit = getIntegerConstant(cursor.getFetchFirstClause(),
                                       "Unsupported LIMIT");
        if (cursor.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new UnsupportedSQLException("Unsupported FOR UPDATE");

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
                                     ((ColumnConditionOperand)
                                      whereCondition.getRight()).getColumn());
        }
    }

    protected BaseJoinNode getJoinNode(FromTable fromTable) throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            TableBinding tb = (TableBinding)fromTable.getUserData();
            if (tb == null) 
                throw new UnsupportedSQLException("Unsupported FROM table", 
                                                  fromTable);
            GroupBinding gb = tb.getGroupBinding();
            if (gb == null)
                throw new UnsupportedSQLException("Unsupported FROM non-group", 
                                                  fromTable);
            if (group == null)
                group = gb;
            else if (group != gb)
                throw new UnsupportedSQLException("Unsupported multiple groups");
            UserTable table = (UserTable)tb.getTable();
            if (!tables.add(table))
                throw new UnsupportedSQLException("Unsupported self join");
            return new TableJoinNode(table);
        }
        else if (fromTable instanceof JoinNode) {
            JoinNode joinNode = (JoinNode)fromTable;
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
            return joinNodes(getJoinNode((FromTable)joinNode.getLeftResultSet()),
                             getJoinNode((FromTable)joinNode.getRightResultSet()),
                             joinType);
        }
        else
            throw new UnsupportedSQLException("Unsupported FROM non-table", fromTable);
    }

    protected BaseJoinNode joinNodes(BaseJoinNode left, BaseJoinNode right, 
                                     JoinType joinType) throws StandardException {
        return new JoinJoinNode(left, right, joinType);
    }

    protected ColumnConditionOperand getColumnConditionOperand(ValueNode operand)
            throws StandardException {
        Column column = getColumnReferenceColumn(operand, null);
        if (column == null) return null;
        return new ColumnConditionOperand(column);
    }

    protected ConditionOperand getConditionOperand(ValueNode operand) 
            throws StandardException {
        if (operand instanceof ColumnReference)
            return new ColumnConditionOperand(getColumnReferenceColumn(operand, 
                                                                       "Unsupported WHERE operand"));
        else if (operand instanceof ConstantNode)
            return new LiteralConditionOperand(((ConstantNode)operand).getValue());
        else if (operand instanceof ParameterNode)
            return new VariableConditionOperand(((ParameterNode)operand).getParameterNumber());
        else
            throw new UnsupportedSQLException("Unsupported WHERE operand",
                                              operand);
    }

    // Get the column that this node references or else return null or throw given error.
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
    
    // Get the constant integer value that this node represents or else throw error.
    protected int getIntegerConstant(ValueNode value, String errmsg) 
            throws StandardException {
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
    public Set<UserTable> getTables() {
        return tables;
    }
    public List<SelectColumn> getSelectColumns() {
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
    public void reorderJoins() throws StandardException {
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
    protected void promoteImpossibleOuterJoins() throws StandardException {
        Set<UserTable> conditionTables = new HashSet<UserTable>();
        for (ColumnCondition condition : conditions) {
            conditionTables.add(condition.getLeft().getColumn().getUserTable());
            if (condition.getRight().isColumn())
                conditionTables.add(((ColumnConditionOperand)
                                     condition.getRight()).getColumn().getUserTable());
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
    protected BaseJoinNode reorderJoinNode(BaseJoinNode join) {
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
                result = new JoinJoinNode(result, joins.get(i), JoinType.INNER);
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

    public String toString() {
        StringBuilder str = new StringBuilder(super.toString());
        str.append("\ngroup: ");
        str.append(group);
        str.append("\njoins: ");
        str.append(joins);
        str.append("\nselect: [");
        for (int i = 0; i < selectColumns.size(); i++) {
            if (i > 0) str.append(", ");
            str.append(selectColumns.get(i));
        }
        str.append("]");
        str.append("\nconditions: ");
        for (int i = 0; i < conditions.size(); i++) {
            if (i > 0) str.append(",\n  ");
            str.append(conditions.get(i));
        }
        if (sortColumns != null) {
            str.append("\nsort: ");
            for (int i = 0; i < sortColumns.size(); i++) {
                if (i > 0) str.append(", ");
                str.append(sortColumns.get(i));
            }
        }
        if (offset > 0) {
            str.append("\noffset: ");
            str.append(offset);
        }
        if (limit >= 0) {
            str.append("\nlimit: ");
            str.append(limit);
        }
        str.append("\nequivalences: ");
        for (int i = 0; i < columnEquivalences.size(); i++) {
            if (i > 0) str.append(",\n  ");
            int j = 0;
            for (Column column : columnEquivalences.get(i)) {
                if (j++ > 0) str.append(" = ");
                str.append(column);
            }
        }
        return str.toString();
    }
}
