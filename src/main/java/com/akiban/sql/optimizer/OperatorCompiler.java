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

import com.akiban.sql.optimizer.SimplifiedSelectQuery.*;

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
public class OperatorCompiler
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
            for (String operator : explainPlan()) {
                if (sb.length() > 0) sb.append("\n");
                sb.append(operator);
            }
            return sb.toString();
        }

        public List<String> explainPlan() {
            List<String> result = new ArrayList<String>();
            explainPlan(resultOperator, result, 0);
            return result;
        }

        protected static void explainPlan(PhysicalOperator operator, 
                                          List<String> into, int depth) {
            
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < depth; i++)
                sb.append("  ");
            sb.append(operator);
            into.add(sb.toString());
            for (PhysicalOperator inputOperator : operator.getInputOperators()) {
                explainPlan(inputOperator, into, depth+1);
            }
        }
    }

    public Result new_compile(CursorNode cursor) throws StandardException {
        // Get into standard form.
        binder.bind(cursor);
        cursor = (CursorNode)booleanNormalizer.normalize(cursor);
        typeComputer.compute(cursor);
        cursor = (CursorNode)subqueryFlattener.flatten(cursor);
        grouper.group(cursor);

        SimplifiedSelectQuery squery = 
            new SimplifiedSelectQuery(cursor, grouper.getJoinConditions());
        GroupBinding group = squery.getGroup();
        GroupTable groupTable = group.getGroup().getGroupTable();
        
        PhysicalOperator resultOperator;
        if (true) {
            resultOperator = groupScan_Default(groupTable);
        }
        
        // TODO: Can apply most Select conditions before flattening.
        // In addition to conditions between fields of different
        // tables, a left join should not be satisfied if the right
        // table has a failing condition, since the WHERE is on the
        // whole (as opposed to the outer join with a subquery
        // containing the condition).

        FlattenState fl = flatten(resultOperator, squery.getJoins());
        resultOperator = fl.getResultOperator();
        RowType resultRowType = fl.getResultRowType();
        Map<UserTable,Integer> fieldOffsets = fl.getFieldOffsets();
        
        for (ColumnCondition condition : squery.getConditions()) {
            Expression predicate = condition.generateExpression(fieldOffsets);
            resultOperator = select_HKeyOrdered(resultOperator,
                                                resultRowType,
                                                predicate);
        }

        int ncols = squery.getSelectColumns().size();
        List<Column> resultColumns = new ArrayList<Column>(ncols);
        for (SelectColumn selectColumn : squery.getSelectColumns()) {
            resultColumns.add(selectColumn.getColumn());
        }
        int[] resultColumnOffsets = new int[ncols];
        for (int i = 0; i < ncols; i++) {
            Column column = resultColumns.get(i);
            UserTable table = column.getUserTable();
            resultColumnOffsets[i] = fieldOffsets.get(table) + column.getPosition();
        }

        return new Result(resultOperator, resultRowType, 
                          resultColumns, resultColumnOffsets);
    }

    // Need to return several values from flatten operation.
    static class FlattenState {
        private PhysicalOperator resultOperator;
        private RowType resultRowType;
        private Map<UserTable,Integer> fieldOffsets = new HashMap<UserTable,Integer>();
        int nfields = 0;

        public FlattenState(PhysicalOperator resultOperator,
                            RowType resultRowType,
                            Map<UserTable,Integer> fieldOffsets,
                            int nfields) {
            this.resultOperator = resultOperator;
            this.resultRowType = resultRowType;
            this.fieldOffsets = fieldOffsets;
            this.nfields = nfields;
        }
        
        public PhysicalOperator getResultOperator() {
            return resultOperator;
        }
        public RowType getResultRowType() {
            return resultRowType;
        }
        public Map<UserTable,Integer> getFieldOffsets() {
            return fieldOffsets;
        }
        public int getNfields() {
            return nfields;
        }

        public void setResultOperator(PhysicalOperator resultOperator) {
            this.resultOperator = resultOperator;
            this.resultRowType = resultOperator.rowType();
        }
        
        public void mergeFields(FlattenState other) {
            for (UserTable table : other.fieldOffsets.keySet()) {
                fieldOffsets.put(table, other.fieldOffsets.get(table) + nfields);
            }
            nfields += other.nfields;
        }
    }

    protected FlattenState flatten(PhysicalOperator resultOperator, BaseJoinNode join) {
        if (join instanceof TableJoinNode) {
            UserTable table = ((TableJoinNode)join).getTable();
            Map<UserTable,Integer> fieldOffsets = new HashMap<UserTable,Integer>();
            fieldOffsets.put(table, 0);
            return new FlattenState(resultOperator,
                                    schema.userTableRowType(table),
                                    fieldOffsets,
                                    table.getColumns().size());
        }
        else {
            JoinJoinNode jjoin = (JoinJoinNode)join;
            BaseJoinNode left = jjoin.getLeft();
            BaseJoinNode right = jjoin.getRight();
            FlattenState fleft = flatten(resultOperator, left);
            FlattenState fright = flatten(fleft.getResultOperator(), right);
            int flags = 0x00;
            switch (jjoin.getJoinType()) {
            case LEFT:
                flags = 0x08;
                break;
            case RIGHT:
                flags = 0x10;
                break;
            }
            // There is one current resultOperator, which gets added
            // to as operators are generated. It's the right because
            // that was done second.
            fleft.setResultOperator(flatten_HKeyOrdered(fright.getResultOperator(),
                                                        fleft.getResultRowType(),
                                                        fright.getResultRowType(),
                                                        flags));
            fleft.mergeFields(fright);
            return fleft;
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

    // TODO: Merge with getIndexComparand
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
        else if (operand instanceof ParameterNode) {
            return variable(((ParameterNode)operand).getParameterNumber());
        }
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
                 ((binop.getRightOperand() instanceof ConstantNode) ||
                  (binop.getRightOperand() instanceof ParameterNode))) ||
                (matchColumnReference(column, binop.getRightOperand()) &&
                 ((binop.getLeftOperand() instanceof ConstantNode) ||
                  (binop.getLeftOperand() instanceof ParameterNode)))) {
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
    
    // TODO: isConstant could be a method on Expression, including all
    // trees whose leaves are literals.
    protected static Expression getIndexComparand(ValueNode node, boolean[] isConstant) 
            throws StandardException {
        if (node instanceof ConstantNode) {
            isConstant[0] = true;
            Object value = ((ConstantNode)node).getValue();
            if (value instanceof Integer)
                value = new Long(((Integer)value).intValue());
            return literal(value);
        }
        else if (node instanceof ParameterNode)
            return variable(((ParameterNode)node).getParameterNumber());
        else
            // TODO: Lots more possibilities here as expressions become more complete.
            // Will probably deserve its own class then.
            return null;
    }

    // TODO: Too much work here dealing with multiple conditions that
    // could have been reconciled earlier as part of normalization.
    // Not general enough to handle expressions that actually compute, rather
    // than fetching a field, constant or parameter.
    protected IndexKeyRange getIndexKeyRange(Index index, 
                                             Set<BinaryOperatorNode> indexConditions) 
            throws StandardException {
        List<IndexColumn> indexColumns = index.getColumns();
        int nkeys = indexColumns.size();
        Expression[] keys = new Expression[nkeys];
        Expression[] lb = null, ub = null;
        boolean lbinc = false, ubinc = false;
        for (int i = 0; i < nkeys; i++) {
            IndexColumn indexColumn = indexColumns.get(i);
            Column column = indexColumn.getColumn();
            Expression eqExpr = null, ltExpr = null, gtExpr = null;
            Comparison ltOp = null, gtOp = null;
            boolean ltConstant = false, gtConstant = false;
            for (BinaryOperatorNode condition : indexConditions) {
                Expression expr = null;
                boolean reverse = false;
                boolean[] isConstant = new boolean[1];
                if (matchColumnReference(column, condition.getLeftOperand())) {
                    expr = getIndexComparand(condition.getRightOperand(), isConstant);
                }
                else if (matchColumnReference(column, condition.getRightOperand())) {
                    expr = getIndexComparand(condition.getLeftOperand(), isConstant);
                    reverse = true;
                }
                if (expr == null)
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
                    if (eqExpr == null)
                        eqExpr = expr;
                    else if (!eqExpr.equals(expr))
                        throw new StandardException("Conflicting equality conditions.");
                    break;
                case LT:
                case LE:
                    {
                        boolean narrower;
                        if (ltExpr == null)
                            narrower = true;
                        else {
                            if (!(isConstant[0] && ltConstant))
                                throw new StandardException("Conflicting inequality conditions.");
                            int comp = ((Comparable)ltExpr.evaluate(null, null))
                                .compareTo(expr.evaluate(null, null));
                            narrower = ((comp > 0) ||
                                        // < with same comparand is narrower than <=.
                                        ((comp == 0) && 
                                         (op == Comparison.LT) && 
                                         (ltOp == Comparison.LE)));
                        }
                        if (narrower) {
                            ltExpr = expr;
                            ltOp = op;
                            ltConstant = isConstant[0];
                        }
                    }
                    break;
                case GT:
                case GE:
                    {
                        boolean narrower;
                        if (gtExpr == null)
                            narrower = true;
                        else {
                            if (!(isConstant[0] && gtConstant))
                                throw new StandardException("Conflicting inequality conditions.");
                            int comp = ((Comparable)gtExpr.evaluate(null, null))
                                .compareTo(expr.evaluate(null, null));
                            narrower = ((comp > 0) ||
                                        // > with same comparand is narrower than >=.
                                        ((comp == 0) && 
                                         (op == Comparison.GT) && 
                                         (ltOp == Comparison.GE)));
                        }
                        if (narrower) {
                            gtExpr = expr;
                            gtOp = op;
                            gtConstant = isConstant[0];
                        }
                    }
                    break;
                }
            }
            if (eqExpr != null) {
                keys[i] = eqExpr;
            }
            else {
                if (gtExpr != null) {
                    if (lb == null) {
                        lb = new Expression[nkeys];
                        System.arraycopy(keys, 0, lb, 0, nkeys);
                    }
                    lb[i] = gtExpr;
                    if (gtOp == Comparison.GE) 
                        lbinc = true;
                }
                if (ltExpr != null) {
                    if (ub == null) {
                        ub = new Expression[nkeys];
                        System.arraycopy(keys, 0, ub, 0, nkeys);
                    }
                    ub[i] = ltExpr;
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

    protected IndexBound getIndexBound(Index index, Expression[] keys) {
        if (keys == null) 
            return null;
        return new IndexBound((UserTable)index.getTable(), 
                              getIndexExpressionRow(index, keys),
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

    protected Row getIndexExpressionRow(Index index, Expression[] keys) {
        return new ExpressionRow(schema.indexRowType(index), keys);
    }

}
