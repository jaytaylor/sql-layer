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

import com.foundationdb.sql.compiler.TypeComputer;

import com.foundationdb.sql.parser.*;

import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.types.DataTypeDescriptor;

import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Table;

import com.foundationdb.server.types.TInstance;

import java.util.ArrayList;
import java.util.List;

/** Calculate types from schema information. */
public class AISTypeComputer extends TypeComputer
{
    public AISTypeComputer() {
    }
    
    @Override
    protected DataTypeDescriptor computeType(ValueNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.COLUMN_REFERENCE:
            return columnReference((ColumnReference)node);
        default:
            return super.computeType(node);
        }
    }

    protected DataTypeDescriptor columnReference(ColumnReference node) 
            throws StandardException {
        ColumnBinding columnBinding = (ColumnBinding)node.getUserData();
        assert (columnBinding != null) : "column is not bound yet";
        return columnBinding.getType();
    }

    @Override
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

}
