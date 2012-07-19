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

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.*;

/** Eliminate DISTINCT from SELECT when result is already distinct.
 *
 * Derby has a somewhat different version of this, but it does not try
 * multiple result tables or outer joins.
 *
 * It would be nicer if this could be an actual rule, but it really
 * has to run before ASTStatementLoader to keep from trying to sort on
 * all the extra columns.
 *
 * Likewise, if this runs before SubqueryFlattener, it can remove a
 * DISTINCT from a derived table and allow it to flatten. If it runs
 * after, it will not be stopped by derived tables that do get
 * flattened.  Really, the two need to cooperate more closely.
 */
public class DistinctEliminator
{
    private SQLParserContext parserContext;
    private FromBaseTable unjoinedTable;
    public DistinctEliminator(SQLParserContext parserContext) {
        this.parserContext = parserContext;
    }

    public DMLStatementNode eliminate(DMLStatementNode stmt) throws StandardException {
        ResultSetNode resultSet = stmt.getResultSetNode();
        if (resultSet.getNodeType() == NodeTypes.SELECT_NODE) {
            selectNode((SelectNode)resultSet);
        }
        return stmt;
    }

    protected void selectNode(SelectNode selectNode) throws StandardException {
        boolean foundSubquery = false;
        for (FromTable fromTable : selectNode.getFromList()) {
            if (fromTable instanceof FromSubquery) {
                ResultSetNode subquery = ((FromSubquery)fromTable).getSubquery();
                if (subquery instanceof SelectNode) {
                    selectNode((SelectNode)subquery);
                }
                foundSubquery = true;
            }
        }
        // May have eliminated from subquery, but can't from main one.
        if (foundSubquery) return;
        
        // Nothing more to do if not distinct or if grouped.
        if (!selectNode.isDistinct() ||
            (selectNode.getGroupByList() != null))
            return;

        unjoinedTable = null;

        ResultColumnList resultColumns = selectNode.getResultColumns();
        AndNode whereConditions = (AndNode)selectNode.getWhereClause();
        for (FromTable fromTable : selectNode.getFromList()) {
            if (!isTableDistinct(fromTable, resultColumns, whereConditions, null)) {
                return;
            }
        }

        // Some table needs to not just be joined, so that don't get fooled by
        // SELECT 1 FROM t1,t2 WHERE t1.pk = t2.pk
        if (unjoinedTable != null)
            // Everything looks distinct already.
            selectNode.clearDistinct();
    }

    protected boolean isTableDistinct(FromTable fromTable,
                                      ResultColumnList resultColumns, 
                                      AndNode whereConditions, AndNode joinConditions)
            throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            TableBinding binding = (TableBinding)fromTable.getUserData();
            if (binding == null) return false;

            return isTableDistinct((FromBaseTable)fromTable, binding, 
                                   resultColumns, whereConditions, joinConditions);
        }
        else if (fromTable instanceof JoinNode) {
            return isJoinDistinct((JoinNode)fromTable, resultColumns, whereConditions);
        }
        else
            return false;
    }

    protected boolean isJoinDistinct(JoinNode join,
                                     ResultColumnList resultColumns, 
                                     AndNode whereConditions)  
            throws StandardException {
        ResultSetNode left = join.getLeftResultSet();
        ResultSetNode right = join.getRightResultSet();
        if (!((left instanceof FromTable) && (right instanceof FromTable)))
            return false;
        FromTable leftTable = (FromTable)left;
        FromTable rightTable = (FromTable)right;
        ValueNode joinClause = join.getJoinClause();
        if ((joinClause != null) && !(joinClause instanceof AndNode))
            return false;
        AndNode joinConditions = (AndNode)joinClause;
        if (join instanceof HalfOuterJoinNode) {
            if (((HalfOuterJoinNode)join).isRightOuterJoin()) {
                return (isTableDistinct(leftTable, resultColumns, 
                                        null, joinConditions) &&
                        isTableDistinct(rightTable, resultColumns, 
                                        whereConditions, null));
            }
            else {
                return (isTableDistinct(leftTable, resultColumns, 
                                        whereConditions, null) &&
                        isTableDistinct(rightTable, resultColumns, 
                                        null, joinConditions));
            }
        }
        else
            return (isTableDistinct(leftTable, resultColumns, 
                                    whereConditions, joinConditions) &&
                    isTableDistinct(rightTable, resultColumns, 
                                    whereConditions, joinConditions));
    }

    protected boolean isTableDistinct(FromBaseTable table, TableBinding binding,
                                      ResultColumnList resultColumns, 
                                      AndNode whereConditions, AndNode joinConditions)
            throws StandardException {
        boolean found = false;
        for (Index index : ((Table)binding.getTable()).getIndexes()) {
            if (!index.isUnique()) continue;
            Set<FromTable> joinTables = null;
            Set<FromTable> columnJoinTables = new HashSet<FromTable>();
            boolean handled = true, joined = false;
            for (IndexColumn indexColumn : index.getKeyColumns()) {
                Column column = indexColumn.getColumn();
                // A table's contribution is distinct if every column
                // in some unique index is not nullable and appears in
                // the select list. More joining (with the same
                // condition) won't introduce duplicates.
                if (!binding.isNullable() && 
                    !column.getNullable() &&
                    columnInResult(column, resultColumns)) {
                    continue;
                }
                // A table is unique (occurs zero or one times) if
                // every column of some unique index participates in
                // an equality constraint either with a constant or
                // with a single other table.

                // At least some cases of unique index columns joined
                // to two or more other tables don't keep it unique,
                // such as:
                //   (1,2) (2,3) (3,1)
                //   (1,2) (2,4) (4,1)
                // So don't bother with those cases.
                columnJoinTables.clear();
                if (!(columnInConditions(column, whereConditions, columnJoinTables) ||
                      columnInConditions(column, joinConditions, columnJoinTables))) {
                    columnJoinTables.remove(table);
                    if (columnJoinTables.isEmpty()) {
                        handled = false;
                        break;
                    }
                    joined = true;
                    if (joinTables == null)
                        joinTables = new HashSet<FromTable>(columnJoinTables);
                    else {
                        joinTables.retainAll(columnJoinTables);
                        if (joinTables.isEmpty()) {
                            handled = false;
                            break;
                        }
                    }
                }
            }
            if (handled) {
                found = true;
                if (!joined && (unjoinedTable == null))
                    unjoinedTable = table;
                if (unjoinedTable != null)
                    break;
                // If needed to join, try other indexes; they might not with same table.
            }
        }
        return found;
    }

    // Does the given column appear in the result set directly?
    protected boolean columnInResult(Column column, ResultColumnList resultColumns)
            throws StandardException {
        for (ResultColumn resultColumn : resultColumns) {
            if (isColumnReference(resultColumn.getExpression(), column)) {
                return true;
            }
        }
        return false;
    }

    // Is there some equality condition on this column in these conditions,
    // either to a constant (return true) or some other table(s) (return them)?
    protected boolean columnInConditions(Column column, AndNode conditions,
                                         Set<FromTable> joinTables) 
            throws StandardException {
        if (conditions != null) {
            while (true) {
                ValueNode leftOperand = conditions.getLeftOperand();
                ValueNode rightOperand = conditions.getRightOperand();
                if (leftOperand.getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                    BinaryComparisonOperatorNode equals = (BinaryComparisonOperatorNode)leftOperand;
                    ValueNode otherOperand = null;
                    if (isColumnReference(equals.getLeftOperand(), column))
                        otherOperand = equals.getRightOperand();
                    else if (isColumnReference(equals.getRightOperand(), column))
                        otherOperand = equals.getLeftOperand();
                    if ((otherOperand instanceof ConstantNode) ||
                        (otherOperand instanceof ParameterNode))
                        return true;
                    else if (otherOperand instanceof ColumnReference) {
                        ColumnBinding columnBinding = (ColumnBinding)
                            ((ColumnReference)otherOperand).getUserData();
                        if (columnBinding != null)
                            joinTables.add(columnBinding.getFromTable());
                    }
                }
                if (rightOperand instanceof AndNode)
                    conditions = (AndNode)rightOperand;
                else
                    break;
            }
        }
        return false;
    }

    // This is a reference to the given column?
    protected boolean isColumnReference(ValueNode value, Column column) 
            throws StandardException {
        if (value instanceof ColumnReference) {
            ColumnBinding columnBinding = (ColumnBinding)
                ((ColumnReference)value).getUserData();
            if ((columnBinding != null) && 
                (column == columnBinding.getColumn())) {
                return true;
            }
        }
        return false;
    }

}
