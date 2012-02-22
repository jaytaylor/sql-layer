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
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.server.service.functions.FunctionsRegistry;

public class NestedResultSetTypeComputer extends FunctionsTypeComputer
{
    public NestedResultSetTypeComputer(FunctionsRegistry functionsRegistry) {
        super(functionsRegistry);
    }

    @Override
    public void compute(StatementNode stmt) throws StandardException {
        stmt.accept(this);
        if (stmt.getNodeType() == NodeTypes.CURSOR_NODE) {
            checkForNestedResultSet(((CursorNode)stmt).getResultSetNode());
        }
    }

    protected void checkForNestedResultSet(ResultSetNode resultSet) 
            throws StandardException {
        for (ResultColumn resultColumn : resultSet.getResultColumns()) {
            ValueNode expression = resultColumn.getExpression();
            if (expression.getNodeType() == NodeTypes.SUBQUERY_NODE) {
                SubqueryNode subqueryNode = (SubqueryNode)expression;
                if (subqueryNode.getSubqueryType() == SubqueryNode.SubqueryType.EXPRESSION) {
                    makeNestedResultSet(subqueryNode);
                    resultColumn.setType(subqueryNode.getType());
                }
            }
        }
    }

    protected void makeNestedResultSet(SubqueryNode subqueryNode) 
            throws StandardException {
        ResultSetNode resultSet = subqueryNode.getResultSet();
        checkForNestedResultSet(resultSet); // Do nested first.

        ResultColumnList resultColumns = resultSet.getResultColumns();
        int ncols = resultColumns.size();
        String[] columnNames = new String[ncols];
        DataTypeDescriptor[] columnTypes = new DataTypeDescriptor[ncols];
        for (int i = 0; i < ncols; i++) {
            ResultColumn resultColumn = resultColumns.get(i);
            columnNames[i] = resultColumn.getName();
            columnTypes[i] = resultColumn.getType();
        }
        subqueryNode.setType(DataTypeDescriptor.getRowMultiSet(columnNames, columnTypes));
    }

}
