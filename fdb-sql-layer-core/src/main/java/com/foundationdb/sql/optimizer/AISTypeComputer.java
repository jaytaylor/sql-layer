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

package com.foundationdb.sql.optimizer;

import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.CharacterTypeAttributes;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;

import com.foundationdb.server.types.TInstance;

import java.util.ArrayList;
import java.util.List;

/** Calculate types from schema information. */
public class AISTypeComputer implements TypeComputer, Visitor
{
    public AISTypeComputer() {
    }

    //
    // TypeComputer
    //

    @Override
    public void compute(QueryTreeNode stmt) throws StandardException {
        stmt.accept(this);
    }

    //
    // Visitor
    //

    public Visitable visit(Visitable node) throws StandardException {
        if(node instanceof ValueNode) {
            // Value nodes compute type if necessary.
            ValueNode valueNode = (ValueNode)node;
            if(valueNode.getType() == null) {
                return setType(valueNode);
            }
        } else {
            // Some structural nodes require special handling.
            switch(((QueryTreeNode)node).getNodeType()) {
            case NodeTypes.FROM_SUBQUERY:
                fromSubquery((FromSubquery)node);
                break;
            case NodeTypes.INSERT_NODE:
                insertNode((InsertNode)node);
                break;
            case NodeTypes.SELECT_NODE:
                selectNode((SelectNode)node);
                break;
            }
        }
        return node;
    }

    @Override
    public boolean skipChildren(Visitable node) {
        return false;
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }

    @Override
    public boolean stopTraversal() {
        return false;
    }

    //
    // Internal
    //

    protected ValueNode collateNode(ExplicitCollateNode node) throws StandardException {
        ValueNode operand = node.getOperand();
        DataTypeDescriptor type = operand.getType();
        if(type != null) {
            CharacterTypeAttributes attrs = CharacterTypeAttributes.forCollation(type.getCharacterAttributes(),
                                                                                 node.getCollation());
            operand.setType(new DataTypeDescriptor(type, attrs));
        }
        return operand;
    }

    protected DataTypeDescriptor columnReference(ColumnReference node) 
            throws StandardException {
        ColumnBinding columnBinding = (ColumnBinding)node.getUserData();
        assert (columnBinding != null) : "column is not bound yet";
        return columnBinding.getType();
    }

    protected void fromSubquery(FromSubquery node) throws StandardException {
        if(node.getResultColumns() != null) {
            ResultColumnList rcl1 = node.getResultColumns();
            ResultColumnList rcl2 = node.getSubquery().getResultColumns();
            int size = rcl1.size();
            for(int i = 0; i < size; i++) {
                rcl1.get(i).setType(rcl2.get(i).getType());
            }
        }
    }

    protected void insertNode(InsertNode node) throws StandardException {
        TableName tableName = node.getTargetTableName();
        Table table = (Table)tableName.getUserData();
        if (table == null) return;
        ResultSetNode source = node.getResultSetNode();
        int ncols = source.getResultColumns().size();
        ResultColumnList targetColumns = node.getTargetColumnList();
        List<Column> columns;
        if (targetColumns != null) {
            if (ncols > targetColumns.size())
                ncols = targetColumns.size();
            columns = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                ColumnBinding cb = (ColumnBinding)
                    targetColumns.get(i).getReference().getUserData();
                columns.add((cb == null) ? null : cb.getColumn());
            }
        }
        else {
            List<Column> allColumns = table.getColumns();
            if (ncols > allColumns.size())
                ncols = allColumns.size();
            columns = new ArrayList<>(ncols);
            for (int i = 0; i < ncols; i++) {
                columns.add(allColumns.get(i));
            }
        }
        for (int i = 0; i < ncols; i++) {
            Column column = columns.get(i);
            if (column == null) continue;
            pushType(source, i, column,
                     ColumnBinding.getType(column, false), column.getType());
        }
    }

    protected DataTypeDescriptor resultColumn(ResultColumn node) throws StandardException {
        ValueNode expr = node.getExpression();
        if(expr == null) {
            return null;
        }
        if((expr.getType() == null) && isParameterOrUntypedNull(expr)) {
            ColumnReference column = node.getReference();
            if(column != null) {
                expr.setType(column.getType());
            }
        }
        return expr.getType();
    }

    protected void selectNode(SelectNode node) throws StandardException {
        // Children first wasn't enough to ensure that subqueries were done first.
        if(node.getResultColumns() != null) {
            node.getResultColumns().accept(this);
        }
    }

    protected DataTypeDescriptor subqueryNode(SubqueryNode node) {
        if(node.getSubqueryType() == SubqueryNode.SubqueryType.EXPRESSION) {
            DataTypeDescriptor colType = node.getResultSet().getResultColumns().get(0).getType();
            if(colType == null) {
                return null;
            } else {
                return colType.getNullabilityType(true);
            }
        } else {
            return new DataTypeDescriptor(TypeId.BOOLEAN_ID, true);
        }
    }

    protected void pushType(ResultSetNode result, int i, Column targetColumn,
                            DataTypeDescriptor sqlType, TInstance type)
            throws StandardException {
        ResultColumn column = result.getResultColumns().get(i);
        if (column.getType() == null) {
            column.setType(sqlType); // Parameters and NULL.
            ValueNode expr = column.getExpression();
            if (expr.getType() == null) {
                expr.setType(sqlType);
            }
            if (expr instanceof ParameterNode) {
                expr.setUserData(type);
            }
            else if (expr instanceof DefaultNode) {
                expr.setUserData(targetColumn);
            }
        }
        else {
            // TODO: Could also add casts here to make types consistent.
        }
        switch (result.getNodeType()) {
        case NodeTypes.ROWS_RESULT_SET_NODE:
            for (ResultSetNode row : ((RowsResultSetNode)result).getRows()) {
                pushType(row, i, targetColumn, sqlType, type);
            }
            break;
        case NodeTypes.UNION_NODE:
        case NodeTypes.INTERSECT_OR_EXCEPT_NODE:
            SetOperatorNode setop = (SetOperatorNode)result;
            pushType(setop.getLeftResultSet(), i, targetColumn, sqlType, type);
            pushType(setop.getRightResultSet(), i, targetColumn, sqlType, type);
            break;
        }
    }

    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch(node.getNodeType()) {
            case NodeTypes.COLUMN_REFERENCE:
                return columnReference((ColumnReference)node);
            case NodeTypes.RESULT_COLUMN:
                return resultColumn((ResultColumn)node);
            case NodeTypes.SUBQUERY_NODE:
                return subqueryNode((SubqueryNode)node);
            default:
                return null;
        }
    }

    protected ValueNode setType(ValueNode node) throws StandardException {
        switch(node.getNodeType()) {
            case NodeTypes.EXPLICIT_COLLATE_NODE:
                return collateNode((ExplicitCollateNode)node);
            default:
                node.setType(computeType(node));
                return node;
        }
    }

    //
    // Static
    //

    protected static boolean isParameterOrUntypedNull(ValueNode node) {
        switch(node.getNodeType()) {
            case NodeTypes.PARAMETER_NODE:
            case NodeTypes.UNTYPED_NULL_CONSTANT_NODE:
                return true;
            default:
                return false;
        }
    }
}

