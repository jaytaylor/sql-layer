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

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.impl.sql.compile.SubqueryNode

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.akiban.sql.optimizer;

import com.akiban.sql.parser.*;

import com.akiban.sql.StandardException;
import com.akiban.sql.types.DataTypeDescriptor;
import com.akiban.sql.types.TypeId;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;

import java.util.*;

/** Flatten subqueries.
 *
 * This presently only handles a subset of the cases that Derby did,
 * just to get a feel for what is involved.
 */
public class SubqueryFlattener
{
    SQLParserContext parserContext;
    NodeFactory nodeFactory;
    public SubqueryFlattener(SQLParserContext parserContext) {
        this.parserContext = parserContext;
        this.nodeFactory = parserContext.getNodeFactory();
    }

    private Stack<SelectNode> selectStack;
    private SelectNode currentSelectNode;

    /** Flatten top-level statement.
     * Expects boolean predicates to already be in CNF.
     * 
     * Requires AIS bindings to already be in place. This is for the
     * uniqueness test. Another approach would be to always put into
     * some kind of join that might need special EXISTS processing (like
     * Derby's) when it actually gets generated.
     */
    public StatementNode flatten(StatementNode stmt) throws StandardException {
        if (stmt.getNodeType() == NodeTypes.CURSOR_NODE) {
            ResultSetNode resultSet = ((CursorNode)stmt).getResultSetNode();
            if (resultSet.getNodeType() == NodeTypes.SELECT_NODE) {
                selectStack = new Stack<SelectNode>();
                currentSelectNode = null;
                selectNode((SelectNode)resultSet);
            }
        }
        return stmt;
    }

    protected void selectNode(SelectNode selectNode) throws StandardException {
        selectStack.push(currentSelectNode);
        currentSelectNode = selectNode;

        // Flatten subqueries in the FROM list.
        Iterator<FromTable> iter = selectNode.getFromList().iterator();
        Collection<FromSubquery> flattenSubqueries = new HashSet<FromSubquery>();
        while (iter.hasNext()) {
            FromTable fromTable = iter.next();
            if ((fromTable instanceof FromSubquery) &&
                flattenableFromSubquery((FromSubquery)fromTable)) {
                flattenSubqueries.add((FromSubquery)fromTable);
                iter.remove();                  // Can be flattened out.
            }
        }
        if (!flattenSubqueries.isEmpty()) {
            for (FromSubquery fromSubquery : flattenSubqueries) {
                ResultSetNode subquery = fromSubquery.getSubquery();
                if (subquery instanceof SelectNode) {
                    SelectNode subselect = (SelectNode)subquery;
                    selectNode.getFromList().addAll(subselect.getFromList());
                    selectNode.setWhereClause(mergeWhereClause(subselect.getWhereClause(),
                                                               selectNode.getWhereClause()));
                }
            }
            // Update column bindings.
            FromSubqueryBindingVisitor visitor =
                new FromSubqueryBindingVisitor(flattenSubqueries);
            selectNode.accept(visitor);
        }

        // After CFN, only possibilities are AND and nothing.
        if (selectNode.getWhereClause() != null) {
            AndNode andNode = (AndNode)selectNode.getWhereClause();
            andNode(andNode);
        }
        currentSelectNode = selectStack.pop();
    }

    // Top-level (within some WHERE clause) AND expression.
    protected void andNode(AndNode andNode) throws StandardException {
        // Left operand might be IN (SELECT ...) or = ANY (SELECT ...)
        ValueNode leftOperand = andNode.getLeftOperand();
        if (leftOperand instanceof SubqueryNode) {
            leftOperand = subqueryNode((SubqueryNode)leftOperand, null);
        }
        else if (leftOperand instanceof BinaryComparisonOperatorNode) {
            BinaryComparisonOperatorNode bc = (BinaryComparisonOperatorNode)leftOperand;
            if (bc.getRightOperand() instanceof SubqueryNode)
                leftOperand = subqueryNode((SubqueryNode)bc.getRightOperand(), bc);
        }
        andNode.setLeftOperand(leftOperand);

        // Right operand is either another AND or constant TRUE.
        if (!andNode.getRightOperand().isBooleanTrue())
            andNode((AndNode)andNode.getRightOperand());
    }

    // Subquery either on RHS of binary comparison or as top-level boolean in WHERE.
    protected ValueNode subqueryNode(SubqueryNode subqueryNode, 
                                     BinaryComparisonOperatorNode parentComparisonOperator)
            throws StandardException {
        ValueNode result = parentComparisonOperator;
        if (result == null)
            result = subqueryNode;

        ResultSetNode resultSet = subqueryNode.getResultSet();
        // Must be simple SELECT
        if (!(resultSet instanceof SelectNode) ||
            (subqueryNode.getOrderByList() != null) ||
            (subqueryNode.getOffset() != null) ||
            (subqueryNode.getFetchFirst() != null))
            return result;

        // Either comparison or IN, EXISTS or ANY (i.e., not ALL or EXPRESSION).
        if (parentComparisonOperator == null) {
            switch (subqueryNode.getSubqueryType()) {
            case IN:
            case EXISTS:
            case EQ_ANY:
            case NE_ANY:
            case GT_ANY:
            case GE_ANY:
            case LT_ANY:
            case LE_ANY:
                break;
            default:
                return result;
            }
        }

        SelectNode selectNode = (SelectNode)resultSet;
        // Process sub-subqueries first.
        selectNode(selectNode);

        // And if any of those survive, give up.
        HasNodeVisitor visitor = new HasNodeVisitor(SubqueryNode.class);
        selectNode.accept(visitor);
        if (visitor.hasNode())
            return result;
        
        // Get left operand, if any (if from comparison, subquery is the current right).
        ValueNode leftOperand = subqueryNode.getLeftOperand();
        if (parentComparisonOperator != null)
            leftOperand = parentComparisonOperator.getLeftOperand();
        // Right operand is whatever subquery selects.
        ValueNode rightOperand = 
            selectNode.getResultColumns().get(0).getExpression();
        
        boolean additionalEQ = false;
        switch (subqueryNode.getSubqueryType()) {
        case IN:
        case EQ_ANY:
            additionalEQ = true;
            break;
        }
        additionalEQ = additionalEQ && ((leftOperand instanceof ConstantNode) ||
                                        (leftOperand instanceof ColumnReference) ||
                                        (leftOperand instanceof ParameterNode));
        
        if (!isUniqueSubquery(selectNode, additionalEQ ? rightOperand : null))
            return result;

        // Yes, we can flatten it.
        currentSelectNode.getFromList().addAll(selectNode.getFromList());
        currentSelectNode.setWhereClause(mergeWhereClause(currentSelectNode.getWhereClause(),
                                                          selectNode.getWhereClause()));
        if (leftOperand == null)
            return (ValueNode)nodeFactory.getNode(NodeTypes.BOOLEAN_CONSTANT_NODE,
                                                  Boolean.TRUE,
                                                  parserContext);

        int nodeType = 0;
        switch (subqueryNode.getSubqueryType()) {
            // TODO: The ALL and NOT_IN cases aren't actually supported here yet.
        case IN:
        case EQ_ANY:
        case NOT_IN:
        case NE_ALL:
            nodeType = NodeTypes.BINARY_EQUALS_OPERATOR_NODE;
            break;

        case NE_ANY:
        case EQ_ALL:
            nodeType = NodeTypes.BINARY_NOT_EQUALS_OPERATOR_NODE;
            break;

        case LE_ANY:
        case GT_ALL:
            nodeType = NodeTypes.BINARY_LESS_EQUALS_OPERATOR_NODE;
            break;

        case LT_ANY:
        case GE_ALL:
            nodeType = NodeTypes.BINARY_LESS_THAN_OPERATOR_NODE;
            break;

        case GE_ANY:
        case LT_ALL:
            nodeType = NodeTypes.BINARY_GREATER_EQUALS_OPERATOR_NODE;
            break;

        case GT_ANY:
        case LE_ALL:
            nodeType = NodeTypes.BINARY_GREATER_THAN_OPERATOR_NODE;
            break;

        default:
            assert false;
        }
        return (ValueNode)nodeFactory.getNode(nodeType,
                                              leftOperand, rightOperand, 
                                              parserContext);
    }

    protected boolean flattenableFromSubquery(FromSubquery fromSubquery)
            throws StandardException {
        if (fromSubquery.getSubquery() instanceof SelectNode) {
            SelectNode selectNode = (SelectNode)fromSubquery.getSubquery();
            if (selectNode.getGroupByList() != null)
                return false;
        }
        // TODO: Need more filtering.
        return true;
    }

    protected ValueNode mergeWhereClause(ValueNode whereClause, ValueNode intoWhereClause)
            throws StandardException {
        if (intoWhereClause == null)
            return whereClause;
        assert (intoWhereClause instanceof AndNode);
        if (whereClause == null)
            return intoWhereClause;
        AndNode parentNode = (AndNode)intoWhereClause;
        while (true) {
            ValueNode rightOperand = parentNode.getRightOperand();
            if (rightOperand.isBooleanTrue())
                break;
            parentNode = (AndNode)rightOperand;
        }
        parentNode.setRightOperand(whereClause);
        return intoWhereClause;
    }

    // To be flattened into normal inner join, results must be unique.
    
    // All of the tables in the FROM list must have a unique index all
    // of whose columns appear in top-level equality conditions with
    // something not from that table and at least one of them the
    // stronger condition of something not in the subquery.
    protected boolean isUniqueSubquery(SelectNode selectNode, ValueNode parentOperand)
            throws StandardException {
        FromList fromList = selectNode.getFromList();
        AndNode whereConditions = (AndNode)selectNode.getWhereClause();
        boolean anyStronger = false;
        boolean[] results = new boolean[2];
        for (FromTable fromTable : fromList) {
            TableBinding binding = (TableBinding)fromTable.getUserData();
            if (binding == null) continue;
            Table table = binding.getTable();
            boolean anyIndex = false;
            for (Index index : table.getIndexes()) {
                if (!index.isUnique()) continue;
                boolean allColumns = true, allStronger = true;
                for (IndexColumn indexColumn : index.getColumns()) {
                    Column column = indexColumn.getColumn();
                    results[0] = results[1] = false;
                    findColumnCondition(fromList, fromTable, column, 
                                        whereConditions, parentOperand,
                                        results);
                    if (!results[0]) {
                        // Failed weaker condition (some column condition free of same table),
                        // index is no good.
                        allColumns = false;
                        break;
                    }
                    if (!results[1]) {
                        // Only failed stronger condition (some column condition free of all tables),
                        // remember but finish index.
                        allStronger = false;
                    }
                }
                if (allColumns) {
                    anyIndex = true;
                    if (allStronger) {
                        anyStronger = true;
                        break;
                    }
                }
            }
            if (!anyIndex)
                return false;
        }
        return anyStronger;
    }

    // Does this column appear in an equality condition, either in the
    // where clause or because of the parent RHS, that is free of the given tables.
    // results[0] is true if free of the specific table.
    // results[1] is true if free of all the tables.
    protected void findColumnCondition(FromList fromList, FromTable fromTable, 
                                       Column column, 
                                       AndNode whereConditions, ValueNode parentOperand,
                                       boolean[] results)
            throws StandardException {
        if (isColumnReference(fromTable, column, parentOperand)) {
            results[0] = results[1] = true; // Totally outside this query.
            return;
        }
        if (whereConditions != null) {
            FromTableBindingVisitor visitor = new FromTableBindingVisitor(fromList, fromTable);
            while (true) {
                ValueNode leftOperand = whereConditions.getLeftOperand();
                ValueNode rightOperand = whereConditions.getRightOperand();
                if (leftOperand.getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                    BinaryComparisonOperatorNode binop = (BinaryComparisonOperatorNode)leftOperand;
                    ValueNode checkOperand = null;
                    // If both left and right are column references, it's a
                    // failure caught by visiting the right.
                    if (isColumnReference(fromTable, column, binop.getLeftOperand()))
                        checkOperand = binop.getRightOperand();
                    else if (isColumnReference(fromTable, column, binop.getRightOperand()))
                        checkOperand = binop.getLeftOperand();
                    if (checkOperand != null) {
                        visitor.reset();
                        checkOperand.accept(visitor);
                        switch (visitor.getFound()) {
                        case NOT_FOUND:
                            results[0] = results[1] = true;
                            return;
                        case FOUND_FROM_LIST:
                            results[0] = true;  // Failed the stronger, but passed the weaker test.
                            break;
                        }
                    }
                }
                if (rightOperand instanceof AndNode)
                    whereConditions = (AndNode)rightOperand;
                else
                    break;
            }
        }
    }

    protected boolean isColumnReference(FromTable fromTable, Column column, ValueNode expr)
            throws StandardException {
        if (!(expr instanceof ColumnReference)) return false;
        ColumnBinding binding = (ColumnBinding)((ColumnReference)expr).getUserData();
        return ((binding != null) && 
                (binding.getFromTable() == fromTable) &&
                (binding.getColumn() == column));
    }

    static class FromTableBindingVisitor implements Visitor {
        enum Found { NOT_FOUND, FOUND_FROM_LIST, FOUND_TABLE };

        protected Found found;
        private FromList fromList;
        private FromTable fromTable;

        public FromTableBindingVisitor(FromList fromList, FromTable fromTable) {
            this.found = Found.NOT_FOUND;
            this.fromList = fromList;
            this.fromTable = fromTable;
        }

        public Visitable visit(Visitable node) {
            if (node instanceof ColumnReference) {
                ColumnBinding binding = (ColumnBinding)((ColumnReference)node).getUserData();
                if (binding != null) {
                    FromTable bft = binding.getFromTable();
                    if (bft == fromTable) {
                        found = Found.FOUND_TABLE;
                    }
                    else if (fromList.indexOf(bft) >= 0) {
                        found = Found.FOUND_FROM_LIST;
                    }
                }
            }
            return node;
        }

        public boolean stopTraversal() {
            return (found == Found.FOUND_TABLE);
        }

        public boolean skipChildren(Visitable node) {
            return false;
        }
        public boolean visitChildrenFirst(Visitable node) {
            return false;
        }

        public Found getFound() {
            return found;
        }
        public void reset() {
            found = Found.NOT_FOUND;
        }
    }

    class FromSubqueryBindingVisitor implements Visitor {
        Collection<FromSubquery> subqueries;
        
        public FromSubqueryBindingVisitor(Collection<FromSubquery> subqueries) {
            this.subqueries = subqueries;
        }

        public Visitable visit(Visitable node) throws StandardException {
            if (node instanceof ColumnReference) {
                ColumnBinding binding = (ColumnBinding)((ColumnReference)node).getUserData();
                if (binding != null) {
                    FromTable bft = binding.getFromTable();
                    if (subqueries.contains(bft)) {
                        FromSubquery fromSubquery = (FromSubquery)bft;
                        ResultColumn rc = binding.getResultColumn();
                        if (fromSubquery.getResultColumns() != null) {
                            // Bound to derived column: switch to definition column.
                            ResultColumnList rcl1 = fromSubquery.getResultColumns();
                            ResultColumnList rcl2 = fromSubquery.getSubquery().getResultColumns();
                            rc = rcl2.get(rcl1.indexOf(rc));
                        }
                        // Return (copy of) subquery's SELECT expression (or VALUES element).
                        // TODO: We depend on the shallow-copied binding user-data being valid
                        // here. Cf. AISBinder's addView() and when it does binding.
                        return nodeFactory.copyNode(rc.getExpression(), parserContext);
                    }
                }
            }
            return node;
        }

        public boolean stopTraversal() {
            return false;
        }
        public boolean skipChildren(Visitable node) {
            return false;
        }
        public boolean visitChildrenFirst(Visitable node) {
            return false;
        }
    }
}
