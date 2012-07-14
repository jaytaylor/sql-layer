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

import com.akiban.sql.compiler.TypeComputer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.UserTable;

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
        UserTable table = (UserTable)tableName.getUserData();
        if (table == null) return;
        ResultSetNode source = node.getResultSetNode();
        int ncols = source.getResultColumns().size();
        ResultColumnList targetColumns = node.getTargetColumnList();
        List<Column> columns;
        if (targetColumns != null) {
            if (ncols > targetColumns.size())
                ncols = targetColumns.size();
            columns = new ArrayList<Column>(ncols);
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
            columns = new ArrayList<Column>(ncols);
            for (int i = 0; i < ncols; i++) {
                columns.add(allColumns.get(i));
            }
        }
        for (int i = 0; i < ncols; i++) {
            Column column = columns.get(i);
            if (column == null) continue;
            pushType(source, i, ColumnBinding.getType(column, false));
        }
    }

    protected void pushType(ResultSetNode result, int i, DataTypeDescriptor type)
            throws StandardException {
        ResultColumn column = result.getResultColumns().get(i);
        if (column.getType() == null) {
            column.setType(type); // Parameters and NULL.
            ValueNode expr = column.getExpression();
            if (expr.getType() == null) {
                expr.setType(type);
            }
        }
        else {
            // TODO: Could also add casts here to make types consistent.
        }
        if (result.getNodeType() == NodeTypes.UNION_NODE) {
            UnionNode union = (UnionNode)result;
            pushType(union.getLeftResultSet(), i, type);
            pushType(union.getRightResultSet(), i, type);
        }
    }

}
