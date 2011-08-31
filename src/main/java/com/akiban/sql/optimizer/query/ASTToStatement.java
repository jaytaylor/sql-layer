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

package com.akiban.sql.optimizer.query;

import com.akiban.sql.optimizer.*;
import com.akiban.sql.optimizer.query.TableTreeBase;

import com.akiban.sql.StandardException;
import com.akiban.sql.parser.*;
import com.akiban.sql.types.DataTypeDescriptor;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Group;
import com.akiban.ais.model.UserTable;

import com.akiban.server.error.NoSuchTableException;
import com.akiban.server.error.ParseException;
import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** Turn a parsed SQL AST into this package's format.
 */
public class ASTToStatement
{
    public static SelectQuery selectQuery(CursorNode cursorNode) {
        try {
            return new ASTToStatement().toSelectQuery(cursorNode);
        }
        catch (StandardException ex) {
            throw new ParseException("", ex.getMessage(), "");
        }
    }

    public static UpdateStatement updateStatement(UpdateNode updateNode) {
        try {
            return new ASTToStatement().toUpdateStatement(updateNode);
        }
        catch (StandardException ex) {
            throw new ParseException("", ex.getMessage(), "");
        }
    }

    public static InsertStatement insertStatement(InsertNode insertNode) {
        try {
            return new ASTToStatement().toInsertStatement(insertNode);
        }
        catch (StandardException ex) {
            throw new ParseException("", ex.getMessage(), "");
        }
    }
    
    public static DeleteStatement deleteStatement(DeleteNode deleteNode) {
        try {
            return new ASTToStatement().toDeleteStatement(deleteNode);
        }
        catch (StandardException ex) {
            throw new ParseException("", ex.getMessage(), "");
        }
    }

    private ASTToStatement() {
    }

    protected SelectQuery toSelectQuery(CursorNode cursorNode) 
            throws StandardException {
        Query query = toQuery(cursorNode);
        if (cursorNode.getOrderByList() != null)
            setOrderBy(query, cursorNode.getOrderByList());
        if ((cursorNode.getOffsetClause() != null) || 
            (cursorNode.getFetchFirstClause() != null))
            setOffsetAndLimit(query, 
                              cursorNode.getOffsetClause(), 
                              cursorNode.getFetchFirstClause());
        if (cursorNode.getUpdateMode() == CursorNode.UpdateMode.UPDATE)
            throw new UnsupportedSQLException("FOR UPDATE", cursorNode);
        return new SelectQuery(query);
    }

    protected UpdateStatement toUpdateStatement(UpdateNode updateNode)
            throws StandardException {
        Query query = toQuery(updateNode);
        TableNode targetTable = getTargetTable(updateNode);
        ResultColumnList rcl = updateNode.getResultSetNode().getResultColumns();
        List<UpdateColumn> updateColumns = 
            new ArrayList<UpdateColumn>(rcl.size());
        for (ResultColumn result : rcl) {
            Column column = getColumnReferenceColumn(result.getReference(),
                                                     "result column");
            BaseExpression value = toExpression(result.getExpression());
            updateColumns.add(new UpdateColumn(column, value));
        }
        return new UpdateStatement(query, targetTable, updateColumns);
    }

    protected InsertStatement toInsertStatement(InsertNode insertNode)
            throws StandardException {
        Query query = toQuery(insertNode);
        if (insertNode.getOrderByList() != null)
            setOrderBy(query, insertNode.getOrderByList());
        if ((insertNode.getOffset() != null) || 
            (insertNode.getFetchFirst() != null))
            setOffsetAndLimit(query, 
                              insertNode.getOffset(), 
                              insertNode.getFetchFirst());
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
    
    protected DeleteStatement toDeleteStatement(DeleteNode deleteNode)
            throws StandardException {
        Query query = toQuery(deleteNode);
        TableNode targetTable = getTargetTable(deleteNode);
        return new DeleteStatement(query, targetTable);
    }

    protected Query toQuery(DMLStatementNode statement)
            throws StandardException {
        return null;
    }

    protected void setOrderBy(Query query, OrderByList orderByList)
            throws StandardException {
        List<OrderByExpression> obs = new ArrayList<OrderByExpression>();
        for (OrderByColumn orderByColumn : orderByList) {
            BaseExpression expression = toExpression(orderByColumn.getExpression());
            // If column has a constant value, there is no need to sort on it.
            if (!expression.isConstant())
                obs.add(new OrderByExpression(expression, orderByColumn.isAscending()));
        }
        if (!obs.isEmpty())
            query.setOrderBy(obs);
    }

    protected void setOffsetAndLimit(Query query, 
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
                limit = getIntegerConstant(offsetClause, 
                                           "LIMIT must be constant integer");
                if (limit < 0)
                    throw new UnsupportedSQLException("LIMIT must not be negative", 
                                                      limitClause);
            }
        }
        query.setOffsetAndLimit(offset, offsetIsParameter,
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

    protected BaseExpression toExpression(ValueNode valueNode)
            throws StandardException {
        return null;
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
    protected int getIntegerConstant(ValueNode value, String errmsg) {
        if (value instanceof NumericConstantNode) {
            Object number = ((NumericConstantNode)value).getValue();
            if (number instanceof Integer)
                return ((Integer)number).intValue();
        }
        throw new UnsupportedSQLException(errmsg, value);
    }
    
}
