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

import com.akiban.sql.views.ViewDefinition;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.Column;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.Type;

import java.util.*;

/** Bind objects to Akiban schema. */
public class AISBinder implements Visitor
{
    private AkibanInformationSchema ais;
    private String defaultSchemaName;
    private Map<TableName,ViewDefinition> views;
    private Stack<BindingContext> bindingContexts;
    private Set<QueryTreeNode> visited;

    public AISBinder(AkibanInformationSchema ais, String defaultSchemaName) {
        this.ais = ais;
        this.defaultSchemaName = defaultSchemaName;
        this.views = new HashMap<TableName,ViewDefinition>();
        this.bindingContexts = new Stack<BindingContext>();
    }

    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
    }

    public void addView(ViewDefinition view) throws StandardException {
        TableName name = view.getName();
        /**
           if (name.getSchemaName() == null)
           name.setSchemaName(defaultSchemaName);
        **/
        if (views.get(name) != null)
            throw new StandardException("View already defined: " + name);
        views.put(name, view);
    }

    public void removeView(TableName name) throws StandardException {
        if (views.remove(name) == null)
            throw new StandardException("View not defined: " + name);
    }

    public void bind(StatementNode stmt) throws StandardException {
        visited = new HashSet<QueryTreeNode>();
        stmt.accept(this);
        visited = null;
    }
    
    /* Hierarchical Visitor */

    public boolean visitBefore(QueryTreeNode node) throws StandardException {
        boolean first = visited.add(node);

        if (first) {
            switch (node.getNodeType()) {
            case NodeTypes.SUBQUERY_NODE:
                subqueryNode((SubqueryNode)node);
                break;
            case NodeTypes.SELECT_NODE:
                selectNode((SelectNode)node);
                break;
            case NodeTypes.COLUMN_REFERENCE:
                columnReference((ColumnReference)node);
                break;
            }
        }

        switch (node.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
        case NodeTypes.FROM_SUBQUERY:
        case NodeTypes.SUBQUERY_NODE:
            pushBindingContext();
        }

        return first;
    }

    public void visitAfter(QueryTreeNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
        case NodeTypes.FROM_SUBQUERY:
        case NodeTypes.SUBQUERY_NODE:
            popBindingContext();
            break;
        }
    }

    /* Specific node types */

    protected void subqueryNode(SubqueryNode subqueryNode) throws StandardException {
        // The LHS of a subquery operator is bound in the outer context.
        if (subqueryNode.getLeftOperand() != null)
            subqueryNode.getLeftOperand().accept(this);

        ResultSetNode resultSet = subqueryNode.getResultSet();
        ResultColumnList resultColumns = resultSet.getResultColumns();
        // The parser does not enforce the fact that a subquery can only
        // return a single column, so we must check here.
        if (resultColumns.size() != 1) {
            throw new StandardException("Subquery must return single column");
        }

        SubqueryNode.SubqueryType subqueryType = subqueryNode.getSubqueryType();
        /* Verify the usage of "*" in the select list:
         *  o    Only valid in EXISTS subqueries
         *  o    If the AllResultColumn is qualified, then we have to verify
         *       that the qualification is a valid exposed name.
         *       NOTE: The exposed name can come from an outer query block.
         */
        verifySelectStarSubquery(resultSet, subqueryType);

        /* For an EXISTS subquery:
         *  o    If the SELECT list is a "*", then we convert it to a true.
         *       (We need to do the conversion since we don't want the "*" to
         *       get expanded.)
         */
        if (subqueryType == SubqueryNode.SubqueryType.EXISTS) {
            resultSet = setResultToBooleanTrueNode(resultSet);
            subqueryNode.setResultSet(resultSet);
        }
    }

    protected void verifySelectStarSubquery(ResultSetNode resultSet, 
                                            SubqueryNode.SubqueryType subqueryType)
            throws StandardException {
        if (resultSet instanceof SetOperatorNode) {
            SetOperatorNode setOperatorNode = (SetOperatorNode)resultSet;
            verifySelectStarSubquery(setOperatorNode.getLeftResultSet(), subqueryType);
            verifySelectStarSubquery(setOperatorNode.getRightResultSet(), subqueryType);
            return;
        }
        if (!(resultSet.getResultColumns().get(0) instanceof AllResultColumn)) {
            return;
        }
        // Select * currently only valid for EXISTS/NOT EXISTS.
        if (subqueryType != SubqueryNode.SubqueryType.EXISTS) {
            throw new StandardException("Cannot SELECT * in non-EXISTS subquery");
        }
    }

    /**
     * Set the result column for the subquery to a boolean true,
     * Useful for transformations such as
     * changing:
     *      where exists (select ... from ...) 
     * to:
     *      where (select true from ...)
     *
     * NOTE: No transformation is performed if the ResultColumn.expression is
     * already the correct boolean constant.
     * 
     * This method is used during binding of EXISTS predicates to map
     * a subquery's result column list into a single TRUE node.  For
     * SELECT and VALUES subqueries this transformation is pretty
     * straightforward.  But for set operators (ex. INTERSECT) we have
     * to do some extra work.    To see why, assume we have the following
     * query:
     *
     *  select * from ( values 'BAD' ) as T
     *      where exists ((values 1) intersect (values 2))
     *
     * If we treated the INTERSECT in this query the same way that we
     * treat SELECT/VALUES subqueries then the above query would get
     * transformed into:
     *
     *  select * from ( values 'BAD' ) as T
     *      where exists ((values TRUE) intersect (values TRUE))
     *
     * Since both children of the INTERSECT would then have the same value,
     * the result of set operation would be a single value (TRUE), which
     * means the WHERE clause would evaluate to TRUE and thus the query
     * would return one row with value 'BAD'.    That would be wrong.
     *
     * To avoid this problem, we internally wrap this SetOperatorNode
     * inside a "SELECT *" subquery and then we change the new SelectNode's
     * result column list (as opposed to *this* nodes' result column list)
     * to a singe boolean true node:
     *
     *  select * from ( values 'BAD' ) as T where exists
     *          SELECT TRUE FROM ((values 1) intersect (values 2))
     *
     * In this case the left and right children of the INTERSECT retain
     * their values, which ensures that the result of the intersect
     * operation will be correct.    Since (1 intersect 2) is an empty
     * result set, the internally generated SELECT node will return
     * zero rows, which in turn means the WHERE predicate will return
     * NULL (an empty result set from a SubqueryNode is treated as NULL
     * at execution time; see impl/sql/execute/AnyResultSet). Since
     * NULL is not the same as TRUE the query will correctly return
     * zero rows.    DERBY-2370.
     *
     * @exception StandardException Thrown on error
     */
    public ResultSetNode setResultToBooleanTrueNode(ResultSetNode resultSet)
            throws StandardException {
        NodeFactory nodeFactory = resultSet.getNodeFactory();
        SQLParserContext parserContext = resultSet.getParserContext();
        if (resultSet instanceof SetOperatorNode) {
            // First create a FromList to hold this node (and only this node).
            FromList fromList = (FromList)nodeFactory.getNode(NodeTypes.FROM_LIST,
                                                              parserContext);
            fromList.addFromTable((SetOperatorNode)resultSet);

            // Now create a ResultColumnList that simply holds the "*".
            ResultColumnList rcl = (ResultColumnList)
                nodeFactory.getNode(NodeTypes.RESULT_COLUMN_LIST,
                                    parserContext);
            ResultColumn allResultColumn = (ResultColumn) 
                nodeFactory.getNode(NodeTypes.ALL_RESULT_COLUMN,
                                    null,
                                    parserContext);
            rcl.addResultColumn(allResultColumn);

            /* Create a new SELECT node of the form:
             *  SELECT * FROM <thisSetOperatorNode>
             */
            resultSet = (ResultSetNode) 
                nodeFactory.getNode(NodeTypes.SELECT_NODE,
                                    rcl,            // ResultColumns
                                    null,           // AGGREGATE list
                                    fromList, // FROM list
                                    null,           // WHERE clause
                                    null,           // GROUP BY list
                                    null,           // having clause
                                    null, /* window list */
                                    parserContext);

            /* And finally, transform the "*" in the new SELECT node
             * into a TRUE constant node.    This ultimately gives us:
             *
             *  SELECT TRUE FROM <thisSetOperatorNode>
             *
             * which has a single result column that is a boolean TRUE
             * constant.    So we're done.
             */
        }

        ResultColumnList resultColumns = resultSet.getResultColumns();
        ResultColumn resultColumn = resultColumns.get(0);
        if (resultColumns.get(0) instanceof AllResultColumn) {
            resultColumn = (ResultColumn)nodeFactory.getNode(NodeTypes.RESULT_COLUMN,
                                                             "",
                                                             null,
                                                             parserContext);
        }
        else if (resultColumn.getExpression().isBooleanTrue()) {
            // Nothing to do if query is already select TRUE ...
            return resultSet;
        }

        BooleanConstantNode booleanNode = (BooleanConstantNode)
            nodeFactory.getNode(NodeTypes.BOOLEAN_CONSTANT_NODE,
                                Boolean.TRUE,
                                parserContext);
        resultColumn.setExpression(booleanNode);
        resultColumn.setType(booleanNode.getType());
        resultColumns.set(0, resultColumn);
        return resultSet;
    }

    protected void selectNode(SelectNode selectNode) throws StandardException {
        FromList fromList = selectNode.getFromList();
        int size = fromList.size();
        for (int i = 0; i < size; i++) {
            FromTable fromTable = fromList.get(i);
            FromTable newFromTable = fromTable(fromTable);
            if (newFromTable != fromTable)
                fromList.set(i, newFromTable);
        }
        for (int i = 0; i < size; i++) {
            addFromTable(fromList.get(i));
        }
        expandAllsAndNameColumns(selectNode.getResultColumns(), fromList);
    }

    // Process a FROM list table, finding the table binding.
    protected FromTable fromTable(FromTable fromTable) throws StandardException {
        switch (fromTable.getNodeType()) {
        case NodeTypes.FROM_BASE_TABLE:
            return fromBaseTable((FromBaseTable)fromTable);
        case NodeTypes.JOIN_NODE:
        case NodeTypes.HALF_OUTER_JOIN_NODE:
            return joinNode((JoinNode)fromTable);
        default:
            // Subqueries in SELECT don't see earlier FROM list tables.
            return (FromTable)fromTable.accept(this);
        }
    }

    protected FromTable fromBaseTable(FromBaseTable fromBaseTable) 
            throws StandardException {
        TableName tableName = fromBaseTable.getOrigTableName();
        ViewDefinition view = views.get(tableName);
        if (view != null)
            // Splice in definition and bind it.
            // For efficiency, bindings are computed on the definition and
            // then remapped in the deep copy if gives out.
            return fromTable(view.getSubquery(this));

        Table table = lookupTableName(tableName);
        tableName.setUserData(table);
        fromBaseTable.setUserData(new TableBinding(table));
        return fromBaseTable;
    }
    
    protected FromTable joinNode(JoinNode joinNode) throws StandardException {
        joinNode.setLeftResultSet(fromTable((FromTable)joinNode.getLeftResultSet()));
        joinNode.setRightResultSet(fromTable((FromTable)joinNode.getRightResultSet()));
        return joinNode;
    }

    protected void addFromTable(FromTable fromTable) throws StandardException {
        if (fromTable instanceof JoinNode) {
            addJoinNode((JoinNode)fromTable);
            return;
        }
        BindingContext bindingContext = getBindingContext();
        bindingContext.tables.add(fromTable);
        if (fromTable.getCorrelationName() != null) {
            if (bindingContext.correlationNames.put(fromTable.getCorrelationName(), 
                                                    fromTable) != null) {
                throw new StandardException("More than one use of " + 
                                            fromTable.getCorrelationName() +
                                            " as correlation name");
            }
        }
    }

    protected void addJoinNode(JoinNode joinNode) throws StandardException {
        FromTable fromLeft = (FromTable)joinNode.getLeftResultSet();
        FromTable fromRight = (FromTable)joinNode.getRightResultSet();
        addFromTable(fromLeft);
        addFromTable(fromRight);
        if (joinNode.getUsingClause() != null) {
            // Replace USING clause with equivalent equality predicates, all bound up.
            NodeFactory nodeFactory = joinNode.getNodeFactory();
            SQLParserContext parserContext = joinNode.getParserContext();
            ValueNode conditions = null;
            for (ResultColumn rc : joinNode.getUsingClause()) {
                String columnName = rc.getName();
                ColumnBinding leftBinding = getColumnBinding(fromLeft, columnName);
                if (leftBinding == null)
                    throw new StandardException("Column " + columnName +
                                                " not found in " + fromLeft.getExposedName());
                ColumnBinding rightBinding = getColumnBinding(fromRight, columnName);
                if (rightBinding == null)
                    throw new StandardException("Column " + columnName +
                                                " not found in " + fromRight.getExposedName());
                ColumnReference leftCR = (ColumnReference)
                    nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                        columnName, leftBinding.getFromTable().getTableName(),
                                        parserContext);
                ColumnReference rightCR = (ColumnReference)
                    nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                        columnName, rightBinding.getFromTable().getTableName(),
                                        parserContext);
                ValueNode condition = (ValueNode)
                    nodeFactory.getNode(NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                                        leftCR, rightCR,
                                        parserContext);
                if (conditions == null) {
                    conditions = condition;
                }
                else {
                    conditions = (AndNode)nodeFactory.getNode(NodeTypes.AND_NODE,
                                                              conditions, condition,
                                                              parserContext);
                }
            }
            if (joinNode.getJoinClause() == null) {
                joinNode.setJoinClause(conditions);
            }
            else {
                joinNode.setJoinClause((AndNode)nodeFactory.getNode(NodeTypes.AND_NODE,
                                                                    joinNode.getJoinClause(),
                                                                    conditions,
                                                                    parserContext));
            }
            joinNode.setUsingClause(null);
        }
        // Take care of any remaining column bindings in the ON clause.
        if (joinNode.getJoinClause() != null)
            joinNode.getJoinClause().accept(this);
    }

    protected void columnReference(ColumnReference columnReference) 
            throws StandardException {
        ColumnBinding columnBinding = (ColumnBinding)columnReference.getUserData();
        if (columnBinding != null)
            return;

        String columnName = columnReference.getColumnName();
        if (columnReference.getTableNameNode() != null) {
            FromTable fromTable = findFromTable(columnReference.getTableNameNode());
            columnBinding = getColumnBinding(fromTable, columnName);
            if (columnBinding == null)
                throw new StandardException("Column " + columnName +
                                            " not found in " + fromTable.getExposedName());
        }
        else {
            for (BindingContext bindingContext : bindingContexts) {
                for (FromTable fromTable : bindingContext.tables) {
                    ColumnBinding aColumnBinding = getColumnBinding(fromTable, columnName);
                    if (aColumnBinding != null) {
                        if (columnBinding != null)
                            throw new StandardException("Column " + columnName + " is ambiguous");
                        else
                            columnBinding = aColumnBinding;
                    }
                }
            }
            if (columnBinding == null)
                throw new StandardException("Column " + columnName + " not found");
        }
        columnReference.setUserData(columnBinding);
    }

    protected Table lookupTableName(TableName tableName)
            throws StandardException {
        String schemaName = tableName.getSchemaName();
        if (schemaName == null)
            schemaName = defaultSchemaName;
        Table result = ais.getUserTable(schemaName, 
                                        // TODO: Akiban DB thinks it's case sensitive.
                                        tableName.getTableName().toLowerCase());
        if (result == null)
            throw new StandardException("Table " + tableName.getFullTableName() +
                                        " not found");
        return result;
    }

    protected FromTable findFromTable(TableName tableNameNode) throws StandardException {
        String schemaName = tableNameNode.getSchemaName();
        String tableName = tableNameNode.getTableName();
        if (schemaName == null) {
            FromTable fromTable = getBindingContext().correlationNames.get(tableName);
            if (fromTable != null)
                return fromTable;

            schemaName = defaultSchemaName;
        }
        FromTable result = null;
        for (BindingContext bindingContext : bindingContexts) {
            for (FromTable fromTable : bindingContext.tables) {
                if ((fromTable instanceof FromBaseTable) &&
                    // Not allowed to reference correlated by underlying name.
                    (fromTable.getCorrelationName() == null)) {
                    FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
                    TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
                    assert (tableBinding != null) : "table not bound yet";
                        Table table = tableBinding.getTable();
                        if (table.getName().getSchemaName().equalsIgnoreCase(schemaName) &&
                            table.getName().getTableName().equalsIgnoreCase(tableName)) {
                            if (result != null)
                                throw new StandardException("Ambiguous table " + tableName);
                            else
                                result = fromBaseTable;
                        }
                }
            }
        }
        if (result == null)
            throw new StandardException("Table " + tableNameNode + " not found");
        return result;
    }

    protected ColumnBinding getColumnBinding(FromTable fromTable, String columnName)
            throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
            TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
            assert (tableBinding != null) : "table not bound yet";
                Table table = tableBinding.getTable();
                Column column = table.getColumn(columnName);
                if (column == null)
                    return null;
                return new ColumnBinding(fromTable, column);
        }
        else if (fromTable instanceof FromSubquery) {
            FromSubquery fromSubquery = (FromSubquery)fromTable;
            ResultColumnList columns = fromSubquery.getResultColumns();
            if (columns == null)
                columns = fromSubquery.getSubquery().getResultColumns();
            ResultColumn resultColumn = columns.getResultColumn(columnName);
            if (resultColumn == null)
                return null;
            return new ColumnBinding(fromTable, resultColumn);
        }
        else if (fromTable instanceof JoinNode) {
            JoinNode joinNode = (JoinNode)fromTable;
            ColumnBinding leftBinding = getColumnBinding((FromTable)joinNode.getLeftResultSet(),
                                                         columnName);
            if (leftBinding != null)
                return leftBinding;
            return getColumnBinding((FromTable)joinNode.getRightResultSet(), columnName);
        }
        else {
            assert false;
            return null;
        }
    }

    /**
     * Expand any *'s in the ResultColumnList.  In addition, we will guarantee that
     * each ResultColumn has a name.    (All generated names will be unique across the
     * entire statement.)
     *
     * @exception StandardException                             Thrown on error
     */
    public void expandAllsAndNameColumns(ResultColumnList rcl, FromList fromList) 
            throws StandardException {
        boolean expanded = false;
        ResultColumnList allExpansion;
        TableName fullTableName;

        for (int index = 0; index < rcl.size(); index++) {
            ResultColumn rc = rcl.get(index);
            if (rc instanceof AllResultColumn) {
                expanded = true;

                fullTableName = rc.getTableNameObject();
                allExpansion = expandAll(fullTableName, fromList);

                // Make sure that every column has a name.
                for (ResultColumn nrc : allExpansion) {
                    guaranteeColumnName(nrc);
                }

                // Replace the AllResultColumn with the expanded list.
                rcl.remove(index);
                for (int inner = 0; inner < allExpansion.size(); inner++) {
                    rcl.add(index + inner, allExpansion.get(inner));
                }
                index += allExpansion.size() - 1;

                // TODO: This is where Derby remembered the original size in
                // case other things get added to the RCL.
            }
            else {
                // Make sure that every column has a name.
                guaranteeColumnName(rc);
            }
        }
    }

    /**
     * Generate a unique (across the entire statement) column name for unnamed
     * ResultColumns
     *
     * @exception StandardException Thrown on error
     */
    protected void guaranteeColumnName(ResultColumn rc) throws StandardException {
        if (rc.getName() == null) {
            rc.setName(((SQLParser)rc.getParserContext()).generateColumnName());
            rc.setNameGenerated(true);
        }
    }

    /**
     * Expand a "*" into the appropriate ResultColumnList. If the "*"
     * is unqualified it will expand into a list of all columns in all
     * of the base tables in the from list at the current nesting level;
     * otherwise it will expand into a list of all of the columns in the
     * base table that matches the qualification.
     * 
     * @param allTableName The qualification on the "*" as a String
     * @param fromList The select list
     *
     * @return ResultColumnList representing expansion
     *
     * @exception StandardException Thrown on error
     */
    protected ResultColumnList expandAll(TableName allTableName, FromList fromList)
            throws StandardException {
        ResultColumnList resultColumnList = null;
        ResultColumnList tempRCList = null;

        for (FromTable fromTable : fromList) {
            tempRCList = getAllResultColumns(allTableName, fromTable);

            if (tempRCList == null)
                continue;

            /* Expand the column list and append to the list that
             * we will return.
             */
            if (resultColumnList == null)
                resultColumnList = tempRCList;
            else
                resultColumnList.addAll(tempRCList);

            // If the "*" is qualified, then we can stop the expansion as
            // soon as we find the matching table.
            if (allTableName != null)
                break;
        }

        // Give an error if the qualification name did not match an exposed name.
        if (resultColumnList == null) {
            throw new StandardException("Table not found: " + allTableName);
        }

        return resultColumnList;
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   FromTable fromTable)
            throws StandardException {
        switch (fromTable.getNodeType()) {
        case NodeTypes.FROM_BASE_TABLE:
            return getAllResultColumns(allTableName, (FromBaseTable)fromTable);
        default:
            return null;
        }
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   FromBaseTable fromTable)
            throws StandardException {
        TableName exposedName = fromTable.getExposedTableName();
        if ((allTableName != null) && !allTableName.equals(exposedName))
            return null;

        NodeFactory nodeFactory = fromTable.getNodeFactory();
        SQLParserContext parserContext = fromTable.getParserContext();
        ResultColumnList rcList = (ResultColumnList)
            nodeFactory.getNode(NodeTypes.RESULT_COLUMN_LIST,
                                parserContext);
        TableBinding tableBinding = (TableBinding)fromTable.getUserData();
        Table table = tableBinding.getTable();
        for (Column column : table.getColumns()) {
            String columnName = column.getName().toUpperCase();
            ValueNode valueNode = (ValueNode)
                nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                    columnName,
                                    exposedName,
                                    parserContext);
            ResultColumn resultColumn = (ResultColumn)
                nodeFactory.getNode(NodeTypes.RESULT_COLUMN,
                                    columnName,
                                    valueNode,
                                    parserContext);
            rcList.addResultColumn(resultColumn);
            // Easy to do binding right here.
            valueNode.setUserData(new ColumnBinding(fromTable, column));
        }
        return rcList;
    }

    protected static class BindingContext {
        Collection<FromTable> tables = new ArrayList<FromTable>();
        Map<String,FromTable> correlationNames = new HashMap<String,FromTable>();
    }

    protected BindingContext getBindingContext() {
        return bindingContexts.peek();
    }
    protected void pushBindingContext() {
        BindingContext next = new BindingContext();
        if (!bindingContexts.empty()) {
            next.correlationNames.putAll(bindingContexts.peek().correlationNames);
        }
        bindingContexts.push(next);
    }
    protected void popBindingContext() {
        bindingContexts.pop();
    }

    /* Visitor interface.
       This is messy. Perhaps there should be an abstract class which makes the common
       Visitor interface into a Hierarchical Vistor pattern. 
    */

    // To understand why this works, see QueryTreeNode.accept().
    public Visitable visit(Visitable node) throws StandardException {
        visitAfter((QueryTreeNode)node);
        return node;
    }

    public boolean skipChildren(Visitable node) throws StandardException {
        return ! visitBefore((QueryTreeNode)node);
    }

    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }
    public boolean stopTraversal() {
        return false;
    }

}
