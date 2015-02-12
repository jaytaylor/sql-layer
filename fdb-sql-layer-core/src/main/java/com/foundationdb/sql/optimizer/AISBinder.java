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

import com.foundationdb.server.error.AmbiguousColumNameException;
import com.foundationdb.server.error.CorrelationNameAlreadyUsedException;
import com.foundationdb.server.error.JoinNodeAdditionException;
import com.foundationdb.server.error.NoSuchColumnException;
import com.foundationdb.server.error.NoSuchFunctionException;
import com.foundationdb.server.error.NoSuchTableException;
import com.foundationdb.server.error.NoTableSpecifiedInQueryException;
import com.foundationdb.server.error.ProcedureCalledAsFunctionException;
import com.foundationdb.server.error.SQLParserInternalException;
import com.foundationdb.server.error.SelectExistsErrorException;
import com.foundationdb.server.error.SubqueryOneColumnException;
import com.foundationdb.server.error.TableIsBadSubqueryException;
import com.foundationdb.server.error.UnsupportedFullOuterJoinException;
import com.foundationdb.server.error.ViewHasBadSubqueryException;
import com.foundationdb.server.error.WholeGroupQueryException;
import com.foundationdb.sql.StandardException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.views.ViewDefinition;
import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Columnar;
import com.foundationdb.ais.model.Join;
import com.foundationdb.ais.model.JoinColumn;
import com.foundationdb.ais.model.Routine;
import com.foundationdb.ais.model.Table;
import com.foundationdb.ais.model.View;

import java.util.*;

public class AISBinder implements Visitor
{
    private AkibanInformationSchema ais;
    private String defaultSchemaName;
    private Deque<BindingContext> bindingContexts;
    private Set<QueryTreeNode> visited;
    private boolean resultColumnsAvailableBroadly;
    private boolean allowSubqueryMultipleColumns;
    private Set<ValueNode> havingClauses;
    private AISBinderContext context;
    private boolean expandViews;
    private FunctionDefined functionDefined;

    public AISBinder(AkibanInformationSchema ais, String defaultSchemaName) {
        this.ais = ais;
        this.defaultSchemaName = defaultSchemaName;
    }

    public String getDefaultSchemaName() {
        return defaultSchemaName;
    }

    public void setDefaultSchemaName(String defaultSchemaName) {
        this.defaultSchemaName = defaultSchemaName;
    }

    public boolean isResultColumnsAvailableBroadly() {
        return resultColumnsAvailableBroadly;
    }

    public void setResultColumnsAvailableBroadly(boolean resultColumnsAvailableBroadly) {
        this.resultColumnsAvailableBroadly = resultColumnsAvailableBroadly;
    }

    public boolean isAllowSubqueryMultipleColumns() {
        return allowSubqueryMultipleColumns;
    }

    public void setAllowSubqueryMultipleColumns(boolean allowSubqueryMultipleColumns) {
        this.allowSubqueryMultipleColumns = allowSubqueryMultipleColumns;
    }

    public interface FunctionDefined {
        public boolean isDefined(String name);
    }

    public void setFunctionDefined(FunctionDefined functionDefined) {
        this.functionDefined = functionDefined;
    }

    public AISBinderContext getContext() {
        return context;
    }

    protected void setContext(AISBinderContext context) {
        this.context = context;
    }

    public void bind(StatementNode stmt) throws StandardException {
        bind(stmt, true);
    }

    public void bind(QueryTreeNode node, boolean expandViews) throws StandardException {
        this.expandViews = expandViews;
        visited = new HashSet<>();
        bindingContexts = new ArrayDeque<>();
        havingClauses = new HashSet<>();
        try {
            node.accept(this);
        }
        finally {
            visited = null;
            bindingContexts = null;
        }
    }
    
    /* Hierarchical Visitor */

    public boolean visitBefore(QueryTreeNode node)  {
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
            case NodeTypes.INSERT_NODE:
            case NodeTypes.UPDATE_NODE:
            case NodeTypes.DELETE_NODE:
                dmlModStatementNode((DMLModStatementNode)node);
                break;
            case NodeTypes.UNION_NODE:
                unionNode((UnionNode)node);
                break;
            case NodeTypes.INTERSECT_OR_EXCEPT_NODE:
                intersectOrExceptNode((IntersectOrExceptNode)node);
                break;
            case NodeTypes.JAVA_TO_SQL_VALUE_NODE:
                javaValueNode(((JavaToSQLValueNode)node).getJavaValueNode());
            }
//TODO Add default case for nodes that are not implemented in either switch so easier to find bug

        }

        switch (node.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
        case NodeTypes.DELETE_NODE:
        case NodeTypes.INSERT_NODE:
        case NodeTypes.UPDATE_NODE:
            pushBindingContext(((DMLStatementNode)node).getResultSetNode());
            break;
        case NodeTypes.FROM_SUBQUERY:
            pushBindingContext(((FromSubquery)node).getSubquery());
            break;
        case NodeTypes.SUBQUERY_NODE:
            pushBindingContext(((SubqueryNode)node).getResultSet());
            break;
        case NodeTypes.ORDER_BY_LIST:
        case NodeTypes.GROUP_BY_LIST:
            getBindingContext().resultColumnsAvailableContext = node;
            break;
        default:
            if (havingClauses.contains(node)) // No special node type.
                getBindingContext().resultColumnsAvailableContext = node;
            break;
        }

        return first;
    }

    public void visitAfter(QueryTreeNode node) {
        switch (node.getNodeType()) {
        case NodeTypes.CURSOR_NODE:
        case NodeTypes.FROM_SUBQUERY:
        case NodeTypes.SUBQUERY_NODE:
        case NodeTypes.DELETE_NODE:
        case NodeTypes.INSERT_NODE:
        case NodeTypes.UPDATE_NODE:
            popBindingContext();
            break;
        case NodeTypes.ORDER_BY_LIST:
        case NodeTypes.GROUP_BY_LIST:
            getBindingContext().resultColumnsAvailableContext = null;
            break;
        default:
            if (havingClauses.contains(node))
                getBindingContext().resultColumnsAvailableContext = null;
            break;
        }
    }

    /* Specific node types */

    protected void subqueryNode(SubqueryNode subqueryNode) {
        // The LHS of a subquery operator is bound in the outer context.
        if (subqueryNode.getLeftOperand() != null) {
            try {
                subqueryNode.getLeftOperand().accept(this);
            } 
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
        }

        ResultSetNode resultSet = subqueryNode.getResultSet();
        ResultColumnList resultColumns = resultSet.getResultColumns();
        // The parser does not enforce the fact that a subquery can only
        // return a single column, so we must check here.
        if (resultColumns.size() != 1) {
            switch (subqueryNode.getSubqueryType()) {
            case IN:
            case NOT_IN:
            case EXISTS:
            case NOT_EXISTS:
                break;
            case EXPRESSION:
                if (allowSubqueryMultipleColumns)
                    break;
                /* else falls through */
            default:
                throw new SubqueryOneColumnException();
            }
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
            try {
                resultSet = setResultToBooleanTrueNode(resultSet);
            } 
            catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            subqueryNode.setResultSet(resultSet);
        }
    }

    protected void verifySelectStarSubquery(ResultSetNode resultSet, 
                                            SubqueryNode.SubqueryType subqueryType) {
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
        if ((subqueryType != SubqueryNode.SubqueryType.EXISTS) &&
            (!allowSubqueryMultipleColumns ||
             (subqueryType != SubqueryNode.SubqueryType.EXPRESSION))) {
            throw new SelectExistsErrorException ();
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
    public ResultSetNode setResultToBooleanTrueNode(ResultSetNode resultSet) throws StandardException {
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

    protected void selectNode(SelectNode selectNode) {
        FromList fromList = selectNode.getFromList();
        int size = fromList.size();
        for (int i = 0; i < size; i++) {
            FromTable fromTable = fromList.get(i);
            FromTable newFromTable = fromTable(fromTable, false);
            if (newFromTable != fromTable)
                fromList.set(i, newFromTable);
        }
        for (int i = 0; i < size; i++) {
            addFromTable(fromList.get(i));
        }
        expandAllsAndNameColumns(selectNode.getResultColumns(), fromList);
        if (selectNode.getHavingClause() != null)
            havingClauses.add(selectNode.getHavingClause());
    }

    // Process a FROM list table, finding the table binding.
    protected FromTable fromTable(FromTable fromTable, boolean nullable) {
        switch (fromTable.getNodeType()) {
        case NodeTypes.FROM_BASE_TABLE:
            return fromBaseTable((FromBaseTable)fromTable, nullable);
        case NodeTypes.JOIN_NODE:
            return joinNode((JoinNode)fromTable, nullable);
        case NodeTypes.HALF_OUTER_JOIN_NODE:
            return joinNode((HalfOuterJoinNode)fromTable, nullable);
        case NodeTypes.FULL_OUTER_JOIN_NODE:
            throw new UnsupportedFullOuterJoinException();
        case NodeTypes.FROM_SUBQUERY:
            return fromSubquery((FromSubquery)fromTable);
        default:
            // Subqueries in SELECT don't see earlier FROM list tables.
            try {
                return (FromTable)fromTable.accept(this);
            } catch (StandardException e) {
                throw new TableIsBadSubqueryException (fromTable.getOrigTableName().getSchemaName(), fromTable.getOrigTableName().getTableName(), e.getMessage());
            }
        }
    }

    protected FromTable fromBaseTable(FromBaseTable fromBaseTable, boolean nullable)  {
        TableName origName = fromBaseTable.getOrigTableName();
        String schemaName = origName.getSchemaName();
        if (schemaName == null)
            schemaName = defaultSchemaName;
        String tableName = origName.getTableName();
        Columnar table = null;
        View view = ais.getView(schemaName, tableName);
        if (view != null) {
            if (!context.isAccessible(view.getName())) {
                view = null;
            }
            else if (expandViews) {
                ViewDefinition viewdef = context.getViewDefinition(view);
                FromSubquery viewSubquery;
                try {
                    viewSubquery = viewdef.copySubquery(fromBaseTable.getParserContext());
                } 
                catch (StandardException ex) {
                    throw new ViewHasBadSubqueryException(origName.toString(),
                                                          ex.getMessage());
                }
                if (fromBaseTable.getCorrelationName() != null)
                    tableName = fromBaseTable.getCorrelationName();
                viewSubquery.setCorrelationName(tableName);
                return fromTable(viewSubquery, false);
            }
            else {
                table = view;   // Shallow reference within another view definition.
            }
        }
        if (table == null)
            table = lookupTableName(origName, schemaName, tableName);
        origName.setUserData(table);
        fromBaseTable.setUserData(new TableBinding(table, nullable));
        return fromBaseTable;
    }
    
    protected FromTable joinNode(JoinNode joinNode, boolean nullable) {
        joinNode.setLeftResultSet(fromTable((FromTable)joinNode.getLeftResultSet(),
                                            nullable));
        joinNode.setRightResultSet(fromTable((FromTable)joinNode.getRightResultSet(),
                                             nullable));
        return joinNode;
    }

    protected FromTable joinNode(HalfOuterJoinNode joinNode, boolean nullable) {
        joinNode.setLeftResultSet(fromTable((FromTable)joinNode.getLeftResultSet(),
                                            nullable || joinNode.isRightOuterJoin()));
        joinNode.setRightResultSet(fromTable((FromTable)joinNode.getRightResultSet(),
                                             nullable || !joinNode.isRightOuterJoin()));
        return joinNode;
    }

    protected FromSubquery fromSubquery(FromSubquery fromSubquery) {
        // Do the subquery body first to get types, etc.
        try {
            fromSubquery.accept(this);

            if (fromSubquery.getResultColumns() == null) {
                ResultSetNode inner = fromSubquery.getSubquery();
                ResultColumnList innerRCL = inner.getResultColumns();
                if (innerRCL != null) {
                    NodeFactory nodeFactory = fromSubquery.getNodeFactory();
                    SQLParserContext parserContext = fromSubquery.getParserContext();
                    ResultColumnList outerRCL = (ResultColumnList)
                        nodeFactory.getNode(NodeTypes.RESULT_COLUMN_LIST, parserContext);
                    int ncols = 0;
                    for (ResultColumn innerColumn : innerRCL) {
                        guaranteeColumnName(innerColumn);
                        ValueNode valueNode = (ValueNode)
                            nodeFactory.getNode(NodeTypes.VIRTUAL_COLUMN_NODE,
                                                inner,
                                                innerColumn,
                                                ++ncols,
                                                parserContext);
                        ResultColumn outerColumn = (ResultColumn)
                            nodeFactory.getNode(NodeTypes.RESULT_COLUMN,
                                                innerColumn.getName(),
                                                valueNode,
                                                parserContext);
                        outerRCL.addResultColumn(outerColumn);
                        //valueNode.setUserData(new ColumnBinding(inner, innerColumn));
                    }
                    fromSubquery.setResultColumns(outerRCL);
                }
            }
        }
        catch (StandardException e) {
            throw new TableIsBadSubqueryException("", fromSubquery.getExposedName(), e.getMessage());
        }

        return fromSubquery;
    }

    protected void addFromTable(FromTable fromTable) {
        if (fromTable instanceof JoinNode) {
            try {
                addJoinNode((JoinNode)fromTable);
            } catch (StandardException e) {
                throw new JoinNodeAdditionException (fromTable.getOrigTableName().getSchemaName(), fromTable.getOrigTableName().getTableName(), e.getMessage());
            }
            return;
        }
        BindingContext bindingContext = getBindingContext();
        bindingContext.tables.add(fromTable);
        if (fromTable.getCorrelationName() != null) {
            FromTable prevTable = 
                bindingContext.correlationNames.put(fromTable.getCorrelationName(),
                                                    fromTable);
            if ((prevTable != null) && bindingContext.tables.contains(prevTable)) {
                // Other occurrence in this same context.
                throw new CorrelationNameAlreadyUsedException(fromTable.getCorrelationName());
            }
        }
    }

    protected void addJoinNode(JoinNode joinNode) throws StandardException {
        FromTable fromLeft = (FromTable)joinNode.getLeftResultSet();
        FromTable fromRight = (FromTable)joinNode.getRightResultSet();
        if (joinNode.getNodeType() == NodeTypes.FULL_OUTER_JOIN_NODE)
        {
            throw new UnsupportedFullOuterJoinException();
        }
        addFromTable(fromLeft);
        addFromTable(fromRight);
        if ((joinNode.getUsingClause() != null) || joinNode.isNaturalJoin()) {
            // Replace USING clause with equivalent equality predicates, all bound up.
            NodeFactory nodeFactory = joinNode.getNodeFactory();
            SQLParserContext parserContext = joinNode.getParserContext();
            ValueNode conditions = null;
            if (joinNode.getUsingClause() != null) {
                for (ResultColumn rc : joinNode.getUsingClause()) {
                    String columnName = rc.getName();
                    ColumnBinding leftBinding = getColumnBinding(fromLeft, columnName);
                    if (leftBinding == null)
                        throw new NoSuchColumnException(columnName, rc);
                    ColumnBinding rightBinding = getColumnBinding(fromRight, columnName);
                    if (rightBinding == null)
                        throw new NoSuchColumnException(columnName, rc);
                    conditions = addJoinEquality(conditions, 
                                                 columnName, leftBinding, rightBinding,
                                                 nodeFactory, parserContext);
                    BindingContext bindingContext = getBindingContext();
                    // USING is tricky case, since join columns do not repeat in expansion.
                    // (see 7.5 of sql1992 spec)
                    if (joinNode.getNodeType() == NodeTypes.FULL_OUTER_JOIN_NODE)
                    {
                        throw new UnsupportedFullOuterJoinException();
                    } else if (joinNode.getNodeType() == NodeTypes.HALF_OUTER_JOIN_NODE &&
                            ((HalfOuterJoinNode)joinNode).isRightOuterJoin())
                    {
                        // We use the value from the right table on a RIGHT OUTER JOIN
                        // so ignore the column in the left table
                        bindingContext.applyUsingColumn(fromLeft, columnName);
                    } else {
                        // We use the value from the left table on an INNER JOIN or
                        // LEFT OUTER JOIN so ignore the column in the right table
                        bindingContext.applyUsingColumn(fromRight, columnName);
                    }
                }
            }
            else if (joinNode.isNaturalJoin()) {
                Map<String,ColumnBinding> leftCols = new HashMap<>();
                getUniqueColumnBindings(fromLeft, leftCols);
                Map<String,ColumnBinding> rightCols = new HashMap<>();
                getUniqueColumnBindings(fromRight, rightCols);
                for (String columnName : leftCols.keySet()) {
                    ColumnBinding rightBinding = rightCols.get(columnName);
                    if (rightBinding == null) continue;
                    ColumnBinding leftBinding = leftCols.get(columnName);
                    conditions = addJoinEquality(conditions, 
                                                 columnName, leftBinding, rightBinding,
                                                 nodeFactory, parserContext);
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
        }
        // Take care of any remaining column bindings in the ON clause.
        if (joinNode.getJoinClause() != null)
            joinNode.getJoinClause().accept(this);
    }

    protected ValueNode addJoinEquality(ValueNode conditions, 
                                        String columnName, ColumnBinding leftBinding, ColumnBinding rightBinding,
                                        NodeFactory nodeFactory, SQLParserContext parserContext) 
            throws StandardException {
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
        return conditions;
    }

    protected boolean columnReferencesView(ColumnBinding columnBinding) {
        return columnBinding.getFromTable() != null &&
                columnBinding.getFromTable().getUserData() instanceof TableBinding &&
                ((TableBinding)columnBinding.getFromTable().getUserData()).getTable() instanceof View;
    }

    protected void columnReference(ColumnReference columnReference) {
        ColumnBinding columnBinding = (ColumnBinding)columnReference.getUserData();
        if (columnBinding != null) {
            if (!expandViews || !columnReferencesView(columnBinding)) {
                return;
            }
            columnBinding = null;
        }

        String columnName = columnReference.getColumnName();
        if (columnReference.getTableNameNode() != null) {
            FromTable fromTable = findFromTable(columnReference.getTableNameNode());
            columnBinding = getColumnBinding(fromTable, columnName);
            if (columnBinding == null)
                throw new NoSuchColumnException(columnName, columnReference);
        }
        else {
            if (resultColumnsAvailable(getBindingContext().resultColumnsAvailableContext,
                                       columnReference)) {
                ResultColumnList resultColumns = getBindingContext().resultColumns;
                if (resultColumns != null) {
                    ResultColumn resultColumn = resultColumns.getResultColumn(columnName);
                    if (resultColumn != null) {
                        if (resultColumn.getExpression() instanceof ColumnReference) {
                            columnBinding = (ColumnBinding)((ColumnReference)resultColumn.getExpression()).getUserData();
                        }
                        if (columnBinding == null)
                            columnBinding = new ColumnBinding(null, resultColumn);
                    }
                }
            }
            if (columnBinding == null) {
                boolean ambiguous = false;
                outer:
                for (BindingContext bindingContext : bindingContexts) {
                    ColumnBinding contextBinding = null;
                    for (FromTable fromTable : bindingContext.tables) {
                        ColumnBinding tableBinding = getColumnBinding(fromTable, columnName);
                        if (tableBinding != null &&
                                !bindingContext.ignoreDueToUsing(fromTable, columnName)) {
                            if (contextBinding != null) {
                                ambiguous = true;
                                break outer;
                            }
                            contextBinding = tableBinding;
                        }
                    }
                    if (contextBinding != null) {
                        columnBinding = contextBinding;
                        break;
                    }
                }
                if (columnBinding == null) {
                    if (ambiguous)
                        throw new AmbiguousColumNameException(columnName, columnReference);
                    else
                        throw new NoSuchColumnException(columnName, columnReference);
                }
            }
        }
        columnReference.setUserData(columnBinding);
    }

    protected boolean resultColumnsAvailable(QueryTreeNode context, ColumnReference columnReference) {
        if (context == null)
            return false;
        if (!resultColumnsAvailableBroadly) {
            // The column ref must be immediately in the order / group by list, not
            // in a sub-expression.
            switch (context.getNodeType()) {
            case NodeTypes.ORDER_BY_LIST:
                for (OrderByColumn column : ((OrderByList)context)) {
                    if (column.getExpression() == columnReference) {
                        return true;
                    }
                }
                break;
            case NodeTypes.GROUP_BY_LIST:
                for (GroupByColumn column : ((GroupByList)context)) {
                    if (column.getColumnExpression() == columnReference) {
                        return true;
                    }
                }
                break;
            }
            return false;
        }
        return true;
    }

    protected Table lookupTableName(TableName origName, String schemaName, String tableName) {
        Table result = ais.getTable(schemaName, tableName);
        if ((result == null) || 
            ((context != null) && !context.isAccessible(result.getName())))
            throw new NoSuchTableException(schemaName, tableName, origName);
        return result;
    }

    protected FromTable findFromTable(TableName tableNameNode){
        String schemaName = tableNameNode.getSchemaName();
        String tableName = tableNameNode.getTableName();
        if (schemaName == null) {
            FromTable fromTable = getBindingContext().correlationNames.get(tableName);
            if (fromTable != null)
                return fromTable;

            schemaName = defaultSchemaName;
        }
        for (BindingContext bindingContext : bindingContexts) {
            for (FromTable fromTable : bindingContext.tables) {
                if ((fromTable instanceof FromBaseTable) &&
                    // Not allowed to reference correlated by underlying name.
                    (fromTable.getCorrelationName() == null)) {
                    FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
                    TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
                    assert (tableBinding != null) : "table not bound yet";
                    Columnar table = tableBinding.getTable();
                    if (table.getName().getSchemaName().equalsIgnoreCase(schemaName) &&
                        table.getName().getTableName().equalsIgnoreCase(tableName)) {
                        return fromBaseTable;
                    }
                }
            }
        }
        throw new NoSuchTableException(schemaName, tableName, tableNameNode);
    }

    protected void getUniqueColumnBindings(FromTable fromTable, 
                                           Map<String,ColumnBinding>  bindings) 
            throws StandardException {
        if (fromTable instanceof FromBaseTable) {
            FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
            TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
            assert (tableBinding != null) : "table not bound yet";
            Columnar table = tableBinding.getTable();
            for (Column column : table.getColumns()) {
                ColumnBinding prev = bindings.put(column.getName(),
                                                  new ColumnBinding(fromTable, column, 
                                                                    tableBinding.isNullable()));
                if (prev != null)
                    throw new StandardException("Duplicate column name " + column.getName() + " not allowed with NATURAL JOIN");
            }
        }
        else if (fromTable instanceof FromSubquery) {
            FromSubquery fromSubquery = (FromSubquery)fromTable;
            for (ResultColumn resultColumn : fromSubquery.getResultColumns()) {
                ColumnBinding prev = bindings.put(resultColumn.getName(),
                                                  new ColumnBinding(fromTable, resultColumn));
                if (prev != null)
                    throw new StandardException("Duplicate column name " + resultColumn.getName() + " not allowed with NATURAL JOIN");
            }
        }
        else if (fromTable instanceof JoinNode) {
            JoinNode joinNode = (JoinNode)fromTable;
            getUniqueColumnBindings((FromTable)joinNode.getLeftResultSet(), bindings);
            getUniqueColumnBindings((FromTable)joinNode.getRightResultSet(), bindings);
        }
    }

    protected ColumnBinding getColumnBinding(FromTable fromTable, String columnName) {
        if (fromTable instanceof FromBaseTable) {
            FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
            TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
            assert (tableBinding != null) : "table not bound yet";
            Columnar table = tableBinding.getTable();
            Column column = table.getColumn(columnName);
            if (column == null)
                return null;
            return new ColumnBinding(fromTable, column, tableBinding.isNullable());
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
            FromTable leftTable = (FromTable) joinNode.getLeftResultSet();
            ColumnBinding leftBinding = getColumnBinding(leftTable, columnName);
            if (leftBinding != null &&
                    !getBindingContext().ignoreDueToUsing(leftTable, columnName)) {
                return leftBinding;
            } else {
                return getColumnBinding((FromTable) joinNode.getRightResultSet(), columnName);
            }
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
    public void expandAllsAndNameColumns(ResultColumnList rcl, FromList fromList) {
        if (rcl == null) return;

        boolean expanded = false;
        ResultColumnList allExpansion;
        TableName fullTableName;

        for (int index = 0; index < rcl.size(); index++) {
            ResultColumn rc = rcl.get(index);
            if (rc instanceof AllResultColumn) {
                AllResultColumn arc = (AllResultColumn)rc;

                expanded = true;

                fullTableName = arc.getTableNameObject();
                boolean recursive = arc.isRecursive();
                if (recursive && !allowSubqueryMultipleColumns) {
                    throw new WholeGroupQueryException();
                }
                allExpansion = expandAll(fullTableName, fromList, recursive);

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
    protected void guaranteeColumnName(ResultColumn rc) {
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
    protected ResultColumnList expandAll(TableName allTableName, FromList fromList, boolean recursive) {
        ResultColumnList resultColumnList = null;
        ResultColumnList tempRCList = null;

        for (FromTable fromTable : fromList) {
            tempRCList = getAllResultColumns(allTableName, fromTable, recursive);

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
            if (allTableName == null) {
                throw new NoTableSpecifiedInQueryException ();
            } else {
                throw new NoSuchTableException(allTableName.getSchemaName(), allTableName.getTableName(), allTableName);
            }
        }

        return resultColumnList;
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   ResultSetNode fromTable,
                                                   boolean recursive) {
        try {
            switch (fromTable.getNodeType()) {
            case NodeTypes.FROM_BASE_TABLE:
                return getAllResultColumns(allTableName, (FromBaseTable)fromTable,
                                           recursive);
            case NodeTypes.JOIN_NODE:
            case NodeTypes.HALF_OUTER_JOIN_NODE:
                return getAllResultColumns(allTableName, (JoinNode)fromTable, recursive);
            case NodeTypes.FROM_SUBQUERY:
                return getAllResultColumns(allTableName, (FromSubquery)fromTable);
            default:
                return null;
            }
        } 
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   FromBaseTable fromTable,
                                                   boolean recursive)
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
        Columnar table = tableBinding.getTable();
        for (Column column : table.getColumns()) {
            String columnName = column.getName();
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
            valueNode.setUserData(new ColumnBinding(fromTable, column, 
                                                    tableBinding.isNullable()));
        }
        if (recursive && (table instanceof Table)) {
            for (Join child : ((Table)table).getChildJoins()) {
                rcList.addResultColumn(childJoinSubquery(fromTable, child));
            }
        }
        return rcList;
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   JoinNode fromJoin,
                                                   boolean recursive)
            throws StandardException {
        ResultColumnList leftRCL = getAllResultColumns(allTableName,
                                                       fromJoin.getLogicalLeftResultSet(),
                                                       recursive);
        ResultColumnList rightRCL = getAllResultColumns(allTableName,
                                                        fromJoin.getLogicalRightResultSet(),
                                                        recursive);

        if (leftRCL == null)
            return rightRCL;
        else if (rightRCL == null)
            return leftRCL;

        if (fromJoin.getUsingClause() == null) {
            leftRCL.addAll(rightRCL);
            return leftRCL;
        } else {
            ResultColumnList joinRCL;
            // USING is tricky case, since join columns do not repeat in expansion.
            // (see 7.5 of sql1992 spec)
            if (fromJoin.getNodeType() == NodeTypes.FULL_OUTER_JOIN_NODE)
            {
                throw new UnsupportedFullOuterJoinException();
            } else if (fromJoin.getNodeType() == NodeTypes.HALF_OUTER_JOIN_NODE && ((HalfOuterJoinNode)fromJoin).isRightOuterJoin())
            {
                joinRCL = rightRCL.getJoinColumns(fromJoin.getUsingClause());
            } else {
                joinRCL = leftRCL.getJoinColumns(fromJoin.getUsingClause());
            }
            leftRCL.removeJoinColumns(fromJoin.getUsingClause());
            joinRCL.addAll(leftRCL);
            rightRCL.removeJoinColumns(fromJoin.getUsingClause());
            joinRCL.addAll(rightRCL);
            return joinRCL;
        }
    }

    protected ResultColumnList getAllResultColumns(TableName allTableName, 
                                                   FromSubquery fromSubquery)
            throws StandardException {
        NodeFactory nodeFactory = fromSubquery.getNodeFactory();
        SQLParserContext parserContext = fromSubquery.getParserContext();
        TableName exposedName = (TableName)
            nodeFactory.getNode(NodeTypes.TABLE_NAME,
                                null,
                                fromSubquery.getExposedName(),
                                parserContext);
        if ((allTableName != null) && !allTableName.equals(exposedName))
            return null;

        ResultColumnList rcList = (ResultColumnList) 
            nodeFactory.getNode(NodeTypes.RESULT_COLUMN_LIST,
                                parserContext);
        for (ResultColumn resultColumn : fromSubquery.getResultColumns()) {
            String columnName = resultColumn.getName();
            boolean isNameGenerated = resultColumn.isNameGenerated();

            TableName tableName = exposedName;
            ValueNode valueNode = (ValueNode) 
                nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                    columnName,
                                    tableName,
                                    parserContext);
            resultColumn = (ResultColumn)
                nodeFactory.getNode(NodeTypes.RESULT_COLUMN,
                                    columnName,
                                    valueNode,
                                    parserContext);

            resultColumn.setNameGenerated(isNameGenerated);
            rcList.add(resultColumn);
        }
        return rcList;
    }

    /** Make a nested result set for this child (and so on recursively). */
    protected ResultColumn childJoinSubquery(FromBaseTable parentTable, Join child) 
            throws StandardException {
        NodeFactory nodeFactory = parentTable.getNodeFactory();
        SQLParserContext parserContext = parentTable.getParserContext();
        Table childAisTable = child.getChild();
        Object childName = nodeFactory.getNode(NodeTypes.TABLE_NAME,
                                               childAisTable.getName().getSchemaName(),
                                               childAisTable.getName().getTableName(),
                                               parserContext);
        FromBaseTable childTable = (FromBaseTable)
            nodeFactory.getNode(NodeTypes.FROM_BASE_TABLE,
                                childName, childAisTable.getName().getTableName(),
                                null,  null, null, parserContext);
        childTable.setUserData(new TableBinding(childAisTable, false));
        ValueNode whereClause = null;
        for (JoinColumn join : child.getJoinColumns()) {
            ColumnReference parentPK = (ColumnReference)
                nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                    join.getParent().getName(),
                                    parentTable.getTableName(),
                                    parserContext);
            parentPK.setUserData(new ColumnBinding(parentTable, join.getParent(), false));
            ColumnReference childFK = (ColumnReference)
                nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                    join.getChild().getName(),
                                    childName,
                                    parserContext);
            childFK.setUserData(new ColumnBinding(childTable, join.getChild(), false));
            ValueNode equals = (ValueNode)
                nodeFactory.getNode(NodeTypes.BINARY_EQUALS_OPERATOR_NODE,
                                    parentPK,
                                    childFK,
                                    parserContext);
            if (whereClause == null) {
                whereClause = equals;
            }
            else {
                whereClause = (ValueNode)
                    nodeFactory.getNode(NodeTypes.AND_NODE,
                                        whereClause, equals, 
                                        parserContext);
            }
        }
        FromList fromList = (FromList)
            nodeFactory.getNode(NodeTypes.FROM_LIST,
                                parserContext);
        fromList.addFromTable(childTable);
        ResultColumnList rcl = getAllResultColumns(null, childTable, true);
        SelectNode selectNode = (SelectNode)
            nodeFactory.getNode(NodeTypes.SELECT_NODE,
                                rcl, null, fromList, whereClause, null, null, null,
                                parserContext);
        SubqueryNode subquery = (SubqueryNode)
            nodeFactory.getNode(NodeTypes.SUBQUERY_NODE,
                                selectNode, SubqueryNode.SubqueryType.EXPRESSION,
                                null, null, null, null,
                                parserContext);
        ResultColumn resultColumn = (ResultColumn)
            nodeFactory.getNode(NodeTypes.RESULT_COLUMN,
                                childAisTable.getNameForOutput(),
                                subquery,
                                parserContext);
        return resultColumn;
    }

    protected void dmlModStatementNode(DMLModStatementNode node) {
        TableName tableName = node.getTargetTableName();
        String schemaName = tableName.getSchemaName();
        if (schemaName == null)
            schemaName = defaultSchemaName;
        Table table = lookupTableName(tableName, schemaName, tableName.getTableName());
        tableName.setUserData(table);
        
        ResultColumnList targetColumns = null;
        if (node instanceof InsertNode)
            targetColumns = ((InsertNode)node).getTargetColumnList();
        else
            targetColumns = node.getResultSetNode().getResultColumns();
        if (targetColumns != null) {
            for (ResultColumn targetColumn : targetColumns) {
                ColumnReference columnReference = targetColumn.getReference();
                String columnName = columnReference.getColumnName();
                Column column = table.getColumn(columnName);
                if (column == null)
                    throw new NoSuchColumnException(columnName, columnReference);
                ColumnBinding columnBinding = new ColumnBinding(null, column, false);
                columnReference.setUserData(columnBinding);
            }
        }
        if (node.getReturningList() != null) {
            ResultColumnList rcl = node.getReturningList();
            NodeFactory nodeFactory = node.getNodeFactory();
            SQLParserContext parserContext = node.getParserContext();
            FromBaseTable fromTable = null;
            FromList fromList = null;
            try {
                fromTable = (FromBaseTable)nodeFactory.getNode(NodeTypes.FROM_BASE_TABLE,
                        tableName, null, rcl, null, null, parserContext);
                fromTable.setUserData(new TableBinding(table, false));

                fromList = (FromList)nodeFactory.getNode(NodeTypes.FROM_LIST, Boolean.FALSE, parserContext);
                fromList.add(fromTable);
            } catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            node.setUserData(fromTable);
            
            for (int index = 0; index < rcl.size(); index ++) {
                ResultColumn rc = rcl.get(index);
                if (rc instanceof AllResultColumn) {
                    AllResultColumn arc = (AllResultColumn)rc;
                    ResultColumnList allExpansion = expandAll(tableName, fromList, false);
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
                } else {
                    // Make sure that every column has a name.
                    guaranteeColumnName(rc);
                }
            }
            pushBindingContext(null);
            getBindingContext().tables.add(fromTable);
            try {
                rcl.accept(this);
            } catch (StandardException ex) {
                throw new SQLParserInternalException(ex);
            }
            popBindingContext();
        }
    }

    protected void unionNode(UnionNode node) {
        pushBindingContext(null);
        try {
            node.getLeftResultSet().accept(this);
            node.setResultColumns(node.copyResultColumnsFromLeft());
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        popBindingContext();
        pushBindingContext(null);
        try {
            node.getRightResultSet().accept(this);
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        popBindingContext();
    }

    protected void intersectOrExceptNode(IntersectOrExceptNode node) {
        pushBindingContext(null);
        try {
            node.getLeftResultSet().accept(this);
            node.setResultColumns(node.copyResultColumnsFromLeft());
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        popBindingContext();
        pushBindingContext(null);
        try {
            node.getRightResultSet().accept(this);
        }
        catch (StandardException ex) {
            throw new SQLParserInternalException(ex);
        }
        popBindingContext();
    }



    protected void javaValueNode(JavaValueNode javaValue) {
        if ((javaValue instanceof StaticMethodCallNode) &&
            (functionDefined != null)) {
            StaticMethodCallNode methodCall = (StaticMethodCallNode)javaValue;
            Routine routine = null;
            if ((methodCall.getProcedureName() != null) &&
                (methodCall.getProcedureName().hasSchema())) {
                // Qualified name is always a routine and an immediate error if not.
                routine = ais.getRoutine(methodCall.getProcedureName().getSchemaName(),
                                         methodCall.getProcedureName().getTableName());
                if ((routine == null) || !context.isAccessible(routine.getName())) {
                    throw new NoSuchFunctionException(methodCall.getProcedureName().toString());
                }
            }
            else if (!functionDefined.isDefined(methodCall.getMethodName())) {
                // Unqualified only if not a built-in function and error deferred.
                routine = ais.getRoutine(defaultSchemaName, methodCall.getMethodName());
                if ((routine != null) && !context.isAccessible(routine.getName())) {
                    routine = null;
                }
            }
            if (routine != null) {
                if (routine.getReturnValue() == null) {
                    throw new ProcedureCalledAsFunctionException(routine.getName());
                }
                methodCall.setUserData(routine);
            }
        }
    }

    protected static class BindingContext {
        Collection<FromTable> tables = new ArrayList<>();
        Map<String,FromTable> correlationNames = new HashMap<>();
        ResultColumnList resultColumns;
        QueryTreeNode resultColumnsAvailableContext;
        private Map<FromTable, Set<String>> ignoredColumnsDueToUsing = new HashMap<>();

        public void applyUsingColumn(FromTable table,  String columnName)
        {
            Set<String> columnNames = ignoredColumnsDueToUsing.get(table);
            if (columnNames == null)
            {
                columnNames = new HashSet<>();
                ignoredColumnsDueToUsing.put(table, columnNames);
            }
            columnNames.add(columnName);
        }

        public boolean ignoreDueToUsing(FromTable table, String columnName) {
            Set<String> columnNames = ignoredColumnsDueToUsing.get(table);
            return columnNames != null && columnNames.contains(columnName);
        }
    }

    protected BindingContext getBindingContext() {
        return bindingContexts.peek();
    }
    protected void pushBindingContext(ResultSetNode resultSet) {
        BindingContext next = new BindingContext();
        if (!bindingContexts.isEmpty()) {
            // Initially inherit all the same correlation names (but can overwrite).
            next.correlationNames.putAll(bindingContexts.peek().correlationNames);
        }
        if (resultSet != null) {
            next.resultColumns = resultSet.getResultColumns();
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
    public Visitable visit(Visitable node) {
        visitAfter((QueryTreeNode)node);
        return node;
    }

    public boolean skipChildren(Visitable node) {
        return ! visitBefore((QueryTreeNode)node);
    }

    public boolean visitChildrenFirst(Visitable node) {
        return true;
    }
    public boolean stopTraversal() {
        return false;
    }

}
