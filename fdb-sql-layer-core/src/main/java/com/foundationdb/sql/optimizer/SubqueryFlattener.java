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

package com.foundationdb.sql.optimizer;

import com.foundationdb.server.error.UnsupportedSQLException;
import com.foundationdb.sql.parser.*;
import com.foundationdb.sql.types.DataTypeDescriptor;
import com.foundationdb.sql.types.TypeId;

import com.foundationdb.sql.StandardException;
import com.foundationdb.ais.model.Column;
import com.foundationdb.ais.model.Index;
import com.foundationdb.ais.model.IndexColumn;
import com.foundationdb.ais.model.Table;

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

    private Stack<SelectNode> selectStack; // Needs to hold nulls.
    private SelectNode currentSelectNode;

    /** Flatten top-level statement.
     * Expects boolean predicates to already be in CNF.
     * 
     * Requires AIS bindings to already be in place. This is for the
     * uniqueness test. Another approach would be to always put into
     * some kind of join that might need special EXISTS processing (like
     * Derby's) when it actually gets generated.
     */
    public DMLStatementNode flatten(DMLStatementNode stmt) throws StandardException {
        ResultSetNode resultSet = stmt.getResultSetNode();
        if (resultSet.getNodeType() == NodeTypes.SELECT_NODE) {
            selectStack = new Stack<>();
            currentSelectNode = null;
            selectNode((SelectNode)resultSet, stmt);
        }
        return stmt;
    }

    protected void selectNode(SelectNode selectNode, QueryTreeNode parentNode) throws StandardException {
        selectStack.push(currentSelectNode);
        currentSelectNode = selectNode;

        // Flatten subqueries in the FROM list.
        Iterator<FromTable> iter = selectNode.getFromList().iterator();
        Collection<FromSubquery> flattenSubqueries = new HashSet<>();
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
            parentNode.accept(visitor);
        }

        // After CNF, only possibilities are AND and nothing.
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
        selectNode(selectNode, subqueryNode);

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
        
        if (leftOperand instanceof ColumnReference && selectNode.getResultColumns().size() != 1) {
            throw new UnsupportedSQLException("Subquery must have one column");
        }
        
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
        if (leftOperand == null) {
            ValueNode node = (ValueNode)nodeFactory.getNode(NodeTypes.BOOLEAN_CONSTANT_NODE, Boolean.TRUE, parserContext);
            node.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
            return node;
        }

        int nodeType = 0;
        if (parentComparisonOperator != null)
            nodeType = parentComparisonOperator.getNodeType();
        else {
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
        }
        // when multiple columns in the leftOperand
        if (leftOperand instanceof RowConstructorNode) {
            RowConstructorNode rcn = (RowConstructorNode)leftOperand;
            if (rcn.listSize() != selectNode.getResultColumns().size()) {
                throw new UnsupportedSQLException("Subquery needs equal number of columns on left and right side of ... IN ... clause ");
            }
            ResultColumnList rcl = selectNode.getResultColumns();
            ValueNode leftO = null, rightO = null;
            // create branch of equivalent relations for the different columns, connected by AndNodes, and ending with a True Node 
            for (int i = rcn.listSize()-1; i >= 0; i--) {
                if (i == rcn.listSize() - 1 ) {
                    rightO = (ValueNode)nodeFactory.getNode(NodeTypes.BOOLEAN_CONSTANT_NODE, Boolean.TRUE, parserContext);
                    rightO.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
                } 
                else {
                    rightO = (ValueNode)nodeFactory.getNode(NodeTypes.AND_NODE, leftO, rightO, parserContext);
                    rightO.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
                }
                leftO = (ValueNode)nodeFactory.getNode(NodeTypes.BINARY_EQUALS_OPERATOR_NODE, rcn.getNodeList().get(i), rcl.get(i).getExpression(), parserContext);
                leftO.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
            }
            leftOperand = leftO;
            rightOperand = rightO;
            nodeType = NodeTypes.AND_NODE;
        }
   
        ValueNode newNode = (ValueNode)nodeFactory.getNode(nodeType, leftOperand, rightOperand, parserContext);
        newNode.setType(new DataTypeDescriptor(TypeId.BOOLEAN_ID, false));
        return newNode;
    }

    protected boolean flattenableFromSubquery(FromSubquery fromSubquery)
            throws StandardException {
        if (fromSubquery.getSubquery() instanceof SelectNode) {
            SelectNode selectNode = (SelectNode)fromSubquery.getSubquery();
            if (selectNode.isDistinct() ||
                (selectNode.getGroupByList() != null) ||
                (selectNode.getHavingClause() != null))
                return false;
        }
        else if ((fromSubquery.getSubquery() instanceof SetOperatorNode) ||
                 (fromSubquery.getSubquery() instanceof RowsResultSetNode)) {
            return false;
        }
        if ((fromSubquery.getOrderByList() != null) ||
            (fromSubquery.getOffset() != null) ||
            (fromSubquery.getFetchFirst() != null))
            return false;
        // TODO: Need more filtering?
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
    
    // All of the tables joined in the subquery must have a unique
    // index all of whose columns appear in top-level equality
    // conditions with something not from that table and at least one
    // of them the stronger condition of something not in the
    // subquery.
    protected boolean isUniqueSubquery(SelectNode selectNode, ValueNode parentOperand)
            throws StandardException {
        List<FromTable> fromTables = new ArrayList<>(); // Tables in the query.
        List<BinaryComparisonOperatorNode> equalityConditions = 
            new ArrayList<>(); // Column conditions.
        if (!innerJoinedFromTables(selectNode.getFromList(), 
                                   fromTables, equalityConditions))
            return false;
        addEqualityConditions((AndNode)selectNode.getWhereClause(), equalityConditions);
        Collection<Collection<ColumnBinding>> equatedColumns = 
            getEquatedColumns(fromTables, equalityConditions);
        Map<ColumnBinding,ColumnEquality> columnEqualities = 
            new HashMap<>();
        boolean anyStronger = false;
        for (FromTable fromTable : fromTables) {
            TableBinding binding = (TableBinding)fromTable.getUserData();
            boolean anyIndex = false;
            for (Index index : ((Table)binding.getTable()).getIndexes()) {
                if (!index.isUnique()) continue;
                boolean allColumns = true, allStronger = true;
                for (IndexColumn indexColumn : index.getKeyColumns()) {
                    Column column = indexColumn.getColumn();
                    ColumnEquality constraint = 
                        getColumnEquality(fromTable, column, 
                                            fromTables, equatedColumns,
                                            equalityConditions, parentOperand,
                                            columnEqualities);
                    if (constraint == ColumnEquality.NONE) {
                        // Failed weaker condition (some column condition free of same table),
                        // index is no good.
                        allColumns = false;
                        break;
                    }
                    if (constraint == ColumnEquality.OTHER_TABLES) {
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

    // Get all the inner joined tables. Return false for any other
    // kind of join or anything not a table.
    protected boolean innerJoinedFromTables(FromList fromList, 
                                            List<FromTable> intoList,
                                            List<BinaryComparisonOperatorNode> equalityConditions)
            throws StandardException {
        for (FromTable fromTable : fromList) {
            if (!innerJoinedFromTables(fromTable, intoList, equalityConditions))
                return false;
        }
        return true;
    }

    protected boolean innerJoinedFromTables(ResultSetNode fromTable, 
                                            List<FromTable> intoList,
                                            List<BinaryComparisonOperatorNode> equalityConditions)
            throws StandardException {
        switch (fromTable.getNodeType()) {
        case NodeTypes.JOIN_NODE:
            {
                JoinNode joinNode = (JoinNode)fromTable;
                if (innerJoinedFromTables(joinNode.getLeftResultSet(),
                                          intoList, equalityConditions) &&
                    innerJoinedFromTables(joinNode.getRightResultSet(),
                                          intoList, equalityConditions)) {
                    addEqualityConditions((AndNode)joinNode.getJoinClause(),
                                          equalityConditions);
                    return true;
                }
            }
        case NodeTypes.FROM_BASE_TABLE:
            if (fromTable.getUserData() != null) {
                intoList.add((FromBaseTable)fromTable);
                return true;
            }
        }
        return false;
    }

    // Get any conditions that seem to equality conditions with a
    // column from the given CNF conditions.
    protected void addEqualityConditions(AndNode conditions,
                                         List<BinaryComparisonOperatorNode> equalityConditions) {
        if (conditions != null) {
            while (true) {
                ValueNode leftOperand = conditions.getLeftOperand();
                ValueNode rightOperand = conditions.getRightOperand();
                if (leftOperand.getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                    BinaryComparisonOperatorNode equals = (BinaryComparisonOperatorNode)leftOperand;
                    if ((equals.getLeftOperand() instanceof ColumnReference) ||
                        (equals.getRightOperand() instanceof ColumnReference))
                        equalityConditions.add(equals);
                }
                if (rightOperand instanceof AndNode)
                    conditions = (AndNode)rightOperand;
                else
                    break;
            }
        }
    }

    // Get columns within the subquery that are equated. Can then use
    // other conditions on either one to fulfill an index.
    protected Collection<Collection<ColumnBinding>> getEquatedColumns(List<FromTable> fromTables, 
                                                                      List<BinaryComparisonOperatorNode> equalityConditions) {
        Collection<Collection<ColumnBinding>> result = 
            new ArrayList<>();
        Iterator<BinaryComparisonOperatorNode> iter = equalityConditions.iterator();
        while (iter.hasNext()) {
            BinaryComparisonOperatorNode equals = iter.next();
            ColumnBinding leftCB = getColumnBinding(equals.getLeftOperand(), fromTables);
            ColumnBinding rightCB = getColumnBinding(equals.getRightOperand(), fromTables);
            if ((leftCB == null) && (rightCB == null)) {
                iter.remove();
            }
            else if ((leftCB != null) && (rightCB != null)) {
                addEquatedColumns(leftCB, rightCB, result);
                iter.remove();
            }
        }
        return result;
    }

    // Add an equation between two columns in the subquery, merging as necessary.
    protected void addEquatedColumns(ColumnBinding cb1, ColumnBinding cb2, 
                                     Collection<Collection<ColumnBinding>> equatedColumns) {
        Collection<ColumnBinding> cc1 = null, cc2 = null;
        for (Collection<ColumnBinding> cc : equatedColumns) {
            if (cc.contains(cb1))
                cc1 = cc;
            if (cc.contains(cb2))
                cc2 = cc;
            if ((cc1 != null) && (cc2 != null)) 
                break;
        }
        if (cc1 != null) {
            if (cc2 != null) {
                if (cc1 != cc2) {
                    equatedColumns.remove(cc2);
                    cc1.addAll(cc2);
                }
            }
            else {
                cc1.add(cb2);
            }
        }
        else if (cc2 != null) {
            cc2.add(cb1);
        }
        else {
            Collection<ColumnBinding> ncc = new HashSet<>(2);
            ncc.add(cb1);
            ncc.add(cb2);
            equatedColumns.add(ncc);
        }
    }

    enum ColumnEquality { NONE, OTHER_TABLES, OUTSIDE };

    // Determine how strongly the given column is constrained by the
    // given equality conditions.
    protected ColumnEquality getColumnEquality(FromTable fromTable, Column column, 
                                               List<FromTable> fromTables, 
                                               Collection<Collection<ColumnBinding>> equatedColumns,
                                               List<BinaryComparisonOperatorNode> equalityConditions, 
                                               ValueNode parentOperand,
                                               Map<ColumnBinding,ColumnEquality> columnEqualities)
            throws StandardException {
        ColumnBinding columnBinding = new ColumnBinding(fromTable, column, false);
        ColumnEquality constraint = columnEqualities.get(columnBinding);
        if (constraint != null)
            return constraint;  // Already computed earlier.
        Collection<ColumnBinding> equated = null;
        for (Collection<ColumnBinding> cc : equatedColumns) {
            if (cc.contains(columnBinding)) {
                equated = cc;
                break;
            }
        }
        if (equated == null)
            equated = Collections.singletonList(columnBinding);
        outside:
        {
            if (isColumnReference(parentOperand, equated)) {
                // Equated via IN with something outside the subquery.
                constraint = ColumnEquality.OUTSIDE;
                break outside;
            }
            if (equated.size() > 1) {
                for (ColumnBinding cb : equated) {
                    if (cb.getFromTable() != fromTable) {
                        constraint = ColumnEquality.OTHER_TABLES;
                        break;
                    }
                }
            }
            FromTableBindingVisitor visitor = new FromTableBindingVisitor(fromTables, 
                                                                          fromTable);
            for (BinaryComparisonOperatorNode equals : equalityConditions) {
                ValueNode checkOperand = null;
                if (isColumnReference(equals.getLeftOperand(), equated))
                    checkOperand = equals.getRightOperand();
                else if (isColumnReference(equals.getRightOperand(), equated))
                    checkOperand = equals.getLeftOperand();
                if (checkOperand != null) {
                    visitor.reset();
                    checkOperand.accept(visitor);
                    switch (visitor.getFound()) {
                    case NOT_FOUND:
                        constraint = ColumnEquality.OUTSIDE;
                        break outside;
                    case FOUND_FROM_LIST:
                        constraint = ColumnEquality.OTHER_TABLES;
                        break;
                    }
                }
            }
        }
        if (constraint  == null)
            constraint = ColumnEquality.NONE;
        for (ColumnBinding cb : equated) {
            ColumnEquality previous = columnEqualities.put(cb, constraint);
            assert (previous == null);
        }
        return constraint;
    }

    protected ColumnBinding getColumnBinding(ValueNode expr,
                                             List<FromTable> fromTables) {
        if (!(expr instanceof ColumnReference)) return null;
        ColumnBinding columnBinding = (ColumnBinding)
            (((ColumnReference)expr).getUserData());
        if ((columnBinding != null) &&
            fromTables.contains(columnBinding.getFromTable()))
            return columnBinding;
        return null;
    }

    protected boolean isColumnReference(ValueNode expr,
                                        Collection<ColumnBinding> equated)
            throws StandardException {
        return ((expr instanceof ColumnReference) &&
                equated.contains(((ColumnReference)expr).getUserData()));
    }

    static class FromTableBindingVisitor implements Visitor {
        enum Found { NOT_FOUND, FOUND_FROM_LIST, FOUND_TABLE };

        protected Found found;
        private List<FromTable> fromTables;
        private FromTable fromTable;

        public FromTableBindingVisitor(List<FromTable> fromTables, FromTable fromTable) {
            this.found = Found.NOT_FOUND;
            this.fromTables = fromTables;
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
                    else if (fromTables.indexOf(bft) >= 0) {
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
