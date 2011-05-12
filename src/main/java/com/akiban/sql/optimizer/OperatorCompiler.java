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
import com.akiban.sql.compiler.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.UserTable;

import com.akiban.server.api.dml.ColumnSelector;

import com.akiban.qp.expression.Comparison;
import com.akiban.qp.expression.Expression;
import com.akiban.qp.expression.IndexBound;
import com.akiban.qp.expression.IndexKeyRange;
import static com.akiban.qp.expression.API.*;

import com.akiban.qp.physicaloperator.PhysicalOperator;
import static com.akiban.qp.physicaloperator.API.*;

import com.akiban.qp.row.Row;
import com.akiban.qp.rowtype.IndexRowType;
import com.akiban.qp.rowtype.RowType;
import com.akiban.qp.rowtype.Schema;
import com.akiban.qp.rowtype.UserTableRowType;

import java.util.*;

/**
 * Compile SQL statements into operator trees.
 */
public abstract class OperatorCompiler
{
    protected SQLParserContext parserContext;
    protected NodeFactory nodeFactory;
    protected AISBinder binder;
    protected AISTypeComputer typeComputer;
    protected BooleanNormalizer booleanNormalizer;
    protected SubqueryFlattener subqueryFlattener;
    protected Grouper grouper;
    protected Schema schema;

    public OperatorCompiler(SQLParser parser, 
                            AkibanInformationSchema ais, String defaultSchemaName) {
        parserContext = parser;
        nodeFactory = parserContext.getNodeFactory();
        binder = new AISBinder(ais, defaultSchemaName);
        parser.setNodeFactory(new BindingNodeFactory(nodeFactory));
        typeComputer = new AISTypeComputer();
        booleanNormalizer = new BooleanNormalizer(parser);
        subqueryFlattener = new SubqueryFlattener(parser);
        grouper = new Grouper(parser);
        schema = new Schema(ais);
    }

    public void addView(ViewDefinition view) throws StandardException {
        binder.addView(view);
    }

    public static class Result {
        private PhysicalOperator resultOperator;
        private RowType resultRowType;
        private List<Column> resultColumns;
        private int[] resultColumnOffsets;

        public Result(PhysicalOperator resultOperator,
                      RowType resultRowType,
                      List<Column> resultColumns,
                      int[] resultColumnOffsets) {
            this.resultOperator = resultOperator;
            this.resultRowType = resultRowType;
            this.resultColumns = resultColumns;
            this.resultColumnOffsets = resultColumnOffsets;
        }

        public PhysicalOperator getResultOperator() {
            return resultOperator;
        }
        public RowType getResultRowType() {
            return resultRowType;
        }
        public List<Column> getResultColumns() {
            return resultColumns;
        }
        public int[] getResultColumnOffsets() {
            return resultColumnOffsets;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            explainPlan(resultOperator, sb, 0);
            return sb.toString();
        }

        protected static void explainPlan(PhysicalOperator operator, 
                                          StringBuilder into, int depth) {
            for (int i = 0; i < depth; i++)
                into.append("  ");
            into.append(operator);
            into.append("\n");
            for (PhysicalOperator inputOperator : operator.getInputOperators()) {
                explainPlan(inputOperator, into, depth+1);
            }
        }
    }

    public Result compile(CursorNode cursor) throws StandardException {
        // Get into bound & grouped form.
        binder.bind(cursor);
        cursor = (CursorNode)booleanNormalizer.normalize(cursor);
        typeComputer.compute(cursor);
        cursor = (CursorNode)subqueryFlattener.flatten(cursor);
        grouper.group(cursor);

        if (cursor.getOrderByList() != null)
            throw new StandardException("Unsupported ORDER BY");
        if (cursor.getOffsetClause() != null)
            throw new StandardException("Unsupported OFFSET");
        if (cursor.getFetchFirstClause() != null)
            throw new StandardException("Unsupported FETCH");
        if (cursor.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new StandardException("Unsupported FOR UPDATE");

        SelectNode select = (SelectNode)cursor.getResultSetNode();
        if (select.getGroupByList() != null)
            throw new StandardException("Unsupported GROUP BY");
        if (select.isDistinct())
            throw new StandardException("Unsupported DISTINCT");
        if (select.hasWindows())
            throw new StandardException("Unsupported WINDOW");

        List<UserTable> tables = new ArrayList<UserTable>();
        GroupBinding group = addTables(select.getFromList(), tables);
        GroupTable groupTable = group.getGroup().getGroupTable();
        Set<BinaryOperatorNode> indexConditions = new HashSet<BinaryOperatorNode>();
        Index index = null;
        if (select.getWhereClause() != null) {
            // TODO: Put ColumnReferences on the left of any condition with constant in WHERE,
            // changing operand as necessary.
            index = pickBestIndex(tables, select.getWhereClause(), indexConditions);
        }
        PhysicalOperator resultOperator;
        if (index == null) {
            resultOperator = groupScan_Default(groupTable);
        }
        else {
            IndexKeyRange indexKeyRange = getIndexKeyRange(index, indexConditions);
            PhysicalOperator indexOperator = indexScan_Default(index, false, indexKeyRange);
            UserTable indexTable = (UserTable) index.getTable();
            UserTableRowType tableType = schema.userTableRowType(indexTable);
            IndexRowType indexType = tableType.indexRowType(index);
            resultOperator = lookup_Default(indexOperator, groupTable, indexType, tableType);
            // All selected rows above this need to be output by hkey left
            // segment random access.
            List<RowType> addAncestors = new ArrayList<RowType>();
            for (UserTable table : tables) {
                if (table == index.getTable())
                    break;
                addAncestors.add(userTableRowType(table));
            }
            if (!addAncestors.isEmpty())
                resultOperator = ancestorLookup_Default(resultOperator, groupTable, 
                                                        userTableRowType((UserTable)index.getTable()),
                                                        addAncestors);
        }
        RowType resultRowType = null;
        Map<UserTable,Integer> fieldOffsets = new HashMap<UserTable,Integer>();
        UserTable prev = null;
        int nfields = 0;
        // TODO: Tables that are only used for join conditions (no
        // predicates or result columns) can be skipped in flatten (and in
        // index ancestors above).
        for (UserTable table : tables) {
            if (prev != null) {
                if (!isAncestorTable(prev, table))
                    throw new StandardException("Unsupported branching group");
                // Join result so far to new child.
                resultOperator = flatten_HKeyOrdered(resultOperator,
                                                     resultRowType,
                                                     userTableRowType(table));
                resultRowType = resultOperator.rowType();
            }
            else {
                resultRowType = userTableRowType(table);
            }
            prev = table;
            fieldOffsets.put(table, nfields);
            nfields += table.getColumns().size();
        }

        ValueNode whereClause = select.getWhereClause();
        while (whereClause != null) {
            if (whereClause.isBooleanTrue()) break;
            if (!(whereClause instanceof AndNode))
                throw new StandardException("Unsupported complex WHERE");
            AndNode andNode = (AndNode)whereClause;
            whereClause = andNode.getRightOperand();
            ValueNode condition = andNode.getLeftOperand();
            if (grouper.getJoinConditions().contains(condition))
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
                throw new StandardException("Unsupported WHERE predicate");
            }
            BinaryOperatorNode binop = (BinaryOperatorNode)condition;
            if (indexConditions.contains(binop))
                continue;
            Expression leftExpr = getExpression(binop.getLeftOperand(), fieldOffsets);
            Expression rightExpr = getExpression(binop.getRightOperand(), fieldOffsets);
            Expression predicate = compare(leftExpr, op, rightExpr);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                resultRowType,
                                                predicate);
        }

        List<Column> resultColumns = new ArrayList<Column>();
        for (ResultColumn result : select.getResultColumns()) {
            if (!(result.getExpression() instanceof ColumnReference))
                throw new StandardException("Unsupported result column: " + result);
            ColumnReference cref = (ColumnReference)result.getExpression();
            ColumnBinding cb = (ColumnBinding)cref.getUserData();
            if (cb == null)
                throw new StandardException("Unsupported result column: " + result);
            Column column = cb.getColumn();
            if (column == null)
                throw new StandardException("Unsupported result column: " + result);
            resultColumns.add(column);
        }
        int ncols = resultColumns.size();
        int[] resultColumnOffsets = new int[ncols];
        for (int i = 0; i < ncols; i++) {
            Column column = resultColumns.get(i);
            UserTable table = column.getUserTable();
            resultColumnOffsets[i] = fieldOffsets.get(table) + column.getPosition();
        }

        return new Result(resultOperator, resultRowType, 
                          resultColumns, resultColumnOffsets);
    }

    protected GroupBinding addTables(FromList fromTables, List<UserTable> tables) 
            throws StandardException {
        GroupBinding group = null;
        for (FromTable fromTable : fromTables) {
            group = addTable(fromTable, tables, group);
        }
        Collections.sort(tables, new Comparator<UserTable>() {
                             public int compare(UserTable t1, UserTable t2) {
                                 return t1.getDepth().compareTo(t2.getDepth());
                             }
                         });
        return group;
    }

    protected GroupBinding addTable(FromTable fromTable, 
                                    List<UserTable> tables, GroupBinding group)
            throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            TableBinding tb = (TableBinding)fromTable.getUserData();
            if (tb == null) 
                throw new StandardException("Unsupported FROM table: " + fromTable);
            GroupBinding gb = tb.getGroupBinding();
            if (gb == null)
                throw new StandardException("Unsupported FROM non-group: " + fromTable);
            if (group == null)
                group = gb;
            else if (group != gb)
                throw new StandardException("Unsupported multiple groups");
            UserTable table = (UserTable)tb.getTable();
            tables.add(table);
        }
        else if (fromTable instanceof JoinNode) {
            if (fromTable instanceof HalfOuterJoinNode)
                throw new StandardException("Unsupported OUTER JOIN: " + fromTable);
            JoinNode joinNode = (JoinNode)fromTable;
            group = addTable((FromTable)joinNode.getLeftResultSet(), tables, group);
            group = addTable((FromTable)joinNode.getRightResultSet(), tables, group);
        }
        else
            throw new StandardException("Unsupported FROM non-table: " + fromTable);
        return group;
    }

    protected UserTableRowType userTableRowType(UserTable table) {
        return schema.userTableRowType(table);
    }

    protected Expression getExpression(ValueNode operand, 
                                       Map<UserTable,Integer> fieldOffsets)
            throws StandardException {
        if ((operand instanceof ColumnReference) &&
            (operand.getUserData() != null)) {
            Column column = ((ColumnBinding)operand.getUserData()).getColumn();
            if (column == null)
                throw new StandardException("Unsupported WHERE predicate on non-column");
            UserTable table = column.getUserTable();
            return field(fieldOffsets.get(table) + column.getPosition());
        }
        else if (operand instanceof ConstantNode) {
            Object value = ((ConstantNode)operand).getValue();
            if (value instanceof Integer)
                value = new Long(((Integer)value).intValue());
            return literal(value);
        }
        // TODO: Parameters: Literals but with later substitution somehow.
        else
            throw new StandardException("Unsupported WHERE predicate on non-constant");
    }

    /** Is t1 an ancestor of t2? */
    protected static boolean isAncestorTable(UserTable t1, UserTable t2) {
        while (true) {
            Join j = t2.getParentJoin();
            if (j == null)
                return false;
            UserTable parent = j.getParent();
            if (parent == null)
                return false;
            if (parent == t1)
                return true;
            t2 = parent;
        }
    }

    protected Index pickBestIndex(List<UserTable> tables, 
                                  ValueNode whereClause,
                                  Set<BinaryOperatorNode> indexConditions) {
        if (whereClause == null) 
            return null;
        
        Index bestIndex = null;
        Set<BinaryOperatorNode> bestIndexConditions = null;
        for (UserTable table : tables) {
            for (Index index : table.getIndexes()) { // TODO: getIndexesIncludingInternal()
                Set<BinaryOperatorNode> matchingConditions = matchIndexConditions(index, 
                                                                                  whereClause);
                if (matchingConditions.size() > ((bestIndex == null) ? 0 : 
                                                 bestIndexConditions.size())) {
                    bestIndex = index;
                    bestIndexConditions = matchingConditions;
                }
            }
        }
        if (bestIndex != null)
            indexConditions.addAll(bestIndexConditions);
        return bestIndex;
    }

    // Return where conditions matching a left subset of index columns of given index.
    protected Set<BinaryOperatorNode> matchIndexConditions(Index index,
                                                           ValueNode whereClause) {
        Set<BinaryOperatorNode> result = null;
        boolean alleq = true;
        for (IndexColumn indexColumn : index.getColumns()) {
            Column column = indexColumn.getColumn();
            Set<BinaryOperatorNode> match = matchColumnConditions(column, whereClause);
            if (match == null)
                break;
            else if (result == null)
                result = match;
            else
                result.addAll(match);
            if (alleq) {
                for (ValueNode condition : match) {
                    switch (condition.getNodeType()) {
                    case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
                        break;
                    default:
                        alleq = false;
                        break;
                    }
                    if (!alleq) break;
                }
            }
            if (!alleq) break;
        }
        if (result == null)
            result = Collections.emptySet();
        return result;
    }

    // Return where conditions matching given column in supported comparison.
    protected Set<BinaryOperatorNode> matchColumnConditions(Column column,
                                                            ValueNode whereClause) {
        Set<BinaryOperatorNode> result = null;
        while (whereClause != null) {
            if (whereClause.isBooleanTrue()) break;
            if (!(whereClause instanceof AndNode)) break;
            AndNode andNode = (AndNode)whereClause;
            whereClause = andNode.getRightOperand();
            ValueNode condition = andNode.getLeftOperand();
            if (grouper.getJoinConditions().contains(condition))
                continue;
            switch (condition.getNodeType()) {
            case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
            case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
            case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
                break;
            default:
                continue;
            }
            BinaryOperatorNode binop = (BinaryOperatorNode)condition;
            if ((matchColumnReference(column, binop.getLeftOperand()) &&
                 (binop.getRightOperand() instanceof ConstantNode)) ||
                (matchColumnReference(column, binop.getRightOperand()) &&
                 (binop.getLeftOperand() instanceof ConstantNode))) {
                if (result == null)
                    result = new HashSet<BinaryOperatorNode>();
                result.add(binop);
            }
        }
        return result;
    }

    protected static boolean matchColumnReference(Column column, ValueNode operand) {
        if (!(operand instanceof ColumnReference))
            return false;
        ColumnBinding cb = (ColumnBinding)operand.getUserData();
        if (cb == null)
            return false;
        return (column == cb.getColumn());
    }
    
    // TODO: Too much work here dealing with multiple conditions that
    // could have been reconciled earlier as part of normalization.
    protected IndexKeyRange getIndexKeyRange(Index index, 
                                             Set<BinaryOperatorNode> indexConditions) 
            throws StandardException {
        List<IndexColumn> indexColumns = index.getColumns();
        int nkeys = indexColumns.size();
        Object[] keys = new Object[nkeys];
        Object[] lb = null, ub = null;
        boolean lbinc = false, ubinc = false;
        for (int i = 0; i < nkeys; i++) {
            IndexColumn indexColumn = indexColumns.get(i);
            Column column = indexColumn.getColumn();
            Object eqValue = null, ltValue = null, gtValue = null;
            Comparison ltOp = null, gtOp = null;
            for (BinaryOperatorNode condition : indexConditions) {
                boolean reverse;
                Object value;
                if (matchColumnReference(column, condition.getLeftOperand()) &&
                    (condition.getRightOperand() instanceof ConstantNode)) {
                    value = ((ConstantNode)condition.getRightOperand()).getValue();
                    reverse = false;
                }
                else if (matchColumnReference(column, condition.getRightOperand()) &&
                         (condition.getLeftOperand() instanceof ConstantNode)) {
                    value = ((ConstantNode)condition.getLeftOperand()).getValue();
                    reverse = true;
                }
                else
                    continue;
                Comparison op;
                switch (condition.getNodeType()) {
                case NodeTypes.BINARY_EQUALS_OPERATOR_NODE:
                    op = Comparison.EQ;
                    break;
                case NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE:
                    op = (reverse) ? Comparison.LT : Comparison.GT;
                    break;
                case NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE:
                    op = (reverse) ? Comparison.LE : Comparison.GE;
                    break;
                case NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE:
                    op = (reverse) ? Comparison.GT : Comparison.LT;
                    break;
                case NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE:
                    op = (reverse) ? Comparison.GE : Comparison.LE;
                    break;
                default:
                    continue;
                }
                switch (op) {
                case EQ:
                    if (eqValue == null)
                        eqValue = value;
                    else if (!eqValue.equals(value))
                        throw new StandardException("Conflicting equality conditions.");
                    break;
                case LT:
                case LE:
                    {
                        int comp = (ltValue == null) ? +1 : ((Comparable)ltValue).compareTo(value);
                        if ((comp > 0) ||
                            ((comp == 0) && (op == Comparison.LT) && (ltOp == Comparison.LE))) {
                            ltValue = value;
                            ltOp = op;
                        }
                    }
                    break;
                case GT:
                case GE:
                    {
                        int comp = (gtValue == null) ? -1 : ((Comparable)gtValue).compareTo(value);
                        if ((comp < 0) ||
                            ((comp == 0) && (op == Comparison.GT) && (gtOp == Comparison.GE))) {
                            gtValue = value;
                            gtOp = op;
                        }
                    }
                    break;
                }
            }
            if (eqValue != null) {
                keys[i] = eqValue;
            }
            else {
                if (gtValue != null) {
                    if (lb == null) {
                        lb = new Object[nkeys];
                        System.arraycopy(keys, 0, lb, 0, nkeys);
                    }
                    lb[i] = gtValue;
                    if (gtOp == Comparison.GE) 
                        lbinc = true;
                }
                if (ltValue != null) {
                    if (ub == null) {
                        ub = new Object[nkeys];
                        System.arraycopy(keys, 0, ub, 0, nkeys);
                    }
                    ub[i] = ltValue;
                    if (ltOp == Comparison.LE) 
                        ubinc = true;
                }
            }
        }
        if ((lb == null) && (ub == null)) {
            IndexBound eq = getIndexBound(index, keys);
            return new IndexKeyRange(eq, true, eq, true);
        }
        else {
            IndexBound lo = getIndexBound(index, lb);
            IndexBound hi = getIndexBound(index, ub);
            return new IndexKeyRange(lo, lbinc, hi, ubinc);
        }
    }

    protected IndexBound getIndexBound(Index index, Object[] keys) {
        if (keys == null) 
            return null;
        return new IndexBound((UserTable)index.getTable(), 
                              getIndexRow(index, keys),
                              getIndexColumnSelector(index));
    }

    protected ColumnSelector getIndexColumnSelector(final Index index) {
        return new ColumnSelector() {
                public boolean includesColumn(int columnPosition) {
                    for (IndexColumn indexColumn : index.getColumns()) {
                        Column column = indexColumn.getColumn();
                        if (column.getPosition() == columnPosition) {
                            return true;
                        }
                    }
                    return false;
                }
            };
    }

    protected abstract Row getIndexRow(Index index, Object[] keys);

}
