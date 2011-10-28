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

import com.akiban.ais.model.Column;
import com.akiban.ais.model.GroupTable;
import com.akiban.ais.model.Join;
import com.akiban.ais.model.JoinColumn;
import com.akiban.ais.model.UserTable;

import java.util.*;

/** Match joined tables to groups. */
public class Grouper implements Visitor
{
    enum VisitMode { GROUP, REWRITE };
    VisitMode visitMode;

    SQLParserContext parserContext;
    NodeFactory nodeFactory;
    public Grouper(SQLParserContext parserContext) {
        this.parserContext = parserContext;
        this.nodeFactory = parserContext.getNodeFactory();
    }

    // Internal state for bound table while finding joins.
    static class BoundTable {
        FromBaseTable baseTable;
        TableBinding tableBinding;
        Map<Column,List<ColumnBinding> > boundColumns;
        Map<ColumnBinding,ValueNode> joinedColumns;
        GroupBinding groupBinding;
        BoundTable(FromBaseTable baseTable, TableBinding tableBinding) {
            this.baseTable = baseTable;
            this.tableBinding = tableBinding;
            boundColumns = new HashMap<Column,List<ColumnBinding> >();
            joinedColumns = new IdentityHashMap<ColumnBinding,ValueNode>();
        }
        UserTable getUserTable() {
            return (UserTable)tableBinding.getTable();
        }
        void setGroupBinding(GroupBinding groupBinding) {
            this.groupBinding = groupBinding;
            tableBinding.setGroupBinding(groupBinding);
        }
    }

    private Map<FromTable,BoundTable> allBoundTables;
    private Set<ValueNode> allJoinConditions;
    private int groupNumber;

    public Set<ValueNode> getJoinConditions() {
        return allJoinConditions;
    }

    public void group(StatementNode stmt) throws StandardException {
        visitMode = VisitMode.GROUP;
        allBoundTables = new HashMap<FromTable,BoundTable>();
        allJoinConditions = new HashSet<ValueNode>();
        groupNumber = 0;
        stmt.accept(this);
        visitMode = null;
    }
    
    public void rewrite(StatementNode stmt) throws StandardException {
        visitMode = VisitMode.REWRITE;
        stmt.accept(this);
        visitMode = null;
    }
    
    /* Group finding */

    protected void groupSelectNode(SelectNode selectNode) throws StandardException {
        Map<FromTable,BoundTable> boundTables = new HashMap<FromTable,BoundTable>();
        for (FromTable fromTable : selectNode.getFromList())
            addFromTable(fromTable, boundTables);
        findJoinedColumns(selectNode.getWhereClause(), boundTables);
        allBoundTables.putAll(boundTables);

        List<BoundTable> ordered = new ArrayList<BoundTable>(boundTables.values());
        Collections.sort(ordered, new Comparator<BoundTable>() {
                             public int compare(BoundTable bt1, BoundTable bt2) {
                                 return bt1.getUserTable().getDepth()
                                     .compareTo(bt2.getUserTable().getDepth());
                             }
                         });
        for (int i = 0; i < ordered.size(); i++) {
            BoundTable boundTable = ordered.get(i);
            UserTable userTable = boundTable.getUserTable();
            Join join = userTable.getParentJoin();
            if (join != null) {
                UserTable parentTable = join.getParent();
                for (int j = 0; j < i; j++) {
                    BoundTable otherBoundTable = ordered.get(j);
                    if (parentTable.equals(otherBoundTable.getUserTable())) {
                        Set<ValueNode> joinConditions = matchJoin(join, boundTable, otherBoundTable);
                        if (joinConditions != null) {
                            boundTable.setGroupBinding(otherBoundTable.groupBinding);
                            allJoinConditions.addAll(joinConditions);
                            break;
                        }
                    }
                }
            }
            if (boundTable.groupBinding == null) {
                boundTable.setGroupBinding(new GroupBinding(userTable.getGroup(),
                                                            "_G_" + (++groupNumber)));
            }
        }
    }

    protected void addFromTable(FromTable fromTable,
                                Map<FromTable,BoundTable> boundTables) {
        if (fromTable instanceof FromBaseTable) {
            FromBaseTable fromBaseTable = (FromBaseTable)fromTable;
            TableBinding tableBinding = (TableBinding)fromBaseTable.getUserData();
            if ((tableBinding != null) &&
                (tableBinding.getTable().getGroup() != null)) {
                BoundTable boundTable = new BoundTable(fromBaseTable, tableBinding);
                boundTables.put(fromBaseTable, boundTable);
            }
        }
        else if (fromTable instanceof JoinNode) {
            // TODO: It may not work to just flatten our accumulation of
            // these, since a different group table in the middle must
            // segregate left and right, unless the join order can be
            // flipped to keep them together.
            JoinNode joinNode = (JoinNode)fromTable;
            addFromTable((FromTable)joinNode.getLeftResultSet(), boundTables);
            addFromTable((FromTable)joinNode.getRightResultSet(), boundTables);
            findJoinedColumns(joinNode.getJoinClause(), boundTables);
        }
    }

    protected void findJoinedColumns(ValueNode condition, 
                                     Map<FromTable,BoundTable> boundTables) {
        while (condition instanceof AndNode) {
            AndNode andNode = (AndNode)condition;
            ValueNode leftOperand = andNode.getLeftOperand();
            do_binop:
            if (leftOperand.getNodeType() == NodeTypes.BINARY_EQUALS_OPERATOR_NODE) {
                BinaryRelationalOperatorNode binop = (BinaryRelationalOperatorNode)leftOperand;
                ValueNode leftEquals = binop.getLeftOperand();
                ValueNode rightEquals = binop.getRightOperand();
                if (!(leftEquals instanceof ColumnReference) ||
                    !(rightEquals instanceof ColumnReference))
                    break do_binop;
                ColumnReference leftCR = (ColumnReference)leftEquals;
                ColumnReference rightCR = (ColumnReference)rightEquals;
                ColumnBinding leftCB = (ColumnBinding)leftCR.getUserData();
                ColumnBinding rightCB = (ColumnBinding)rightCR.getUserData();
                if ((leftCB == null) || (rightCB == null))
                    break do_binop;
                BoundTable leftBT = boundTables.get(leftCB.getFromTable());
                BoundTable rightBT = boundTables.get(rightCB.getFromTable());
                if ((leftBT == null) || (rightBT == null))
                    break do_binop;
                addJoinedColumn(leftCB, leftBT, binop);
                addJoinedColumn(rightCB, rightBT, binop);
            }
            ValueNode rightOperand = andNode.getRightOperand();
            if (rightOperand.isBooleanTrue()) break;
            condition = rightOperand;
        }
    }

    protected void addJoinedColumn(ColumnBinding columnBinding,
                                   BoundTable boundTable,
                                   ValueNode equals) {
        Column column = columnBinding.getColumn();
        List<ColumnBinding> list = boundTable.boundColumns.get(column);
        if (list == null) {
            list = new ArrayList<ColumnBinding>(1);
            boundTable.boundColumns.put(column, list);
        }
        list.add(columnBinding);
        boundTable.joinedColumns.put(columnBinding, equals);
    }

    // Match given join to equality conditions.
    protected Set<ValueNode> matchJoin(Join join, 
                                       BoundTable childBoundTable, 
                                       BoundTable parentBoundTable) {
        Set<ValueNode> result = null;
        for (JoinColumn joinColumn : join.getJoinColumns()) {
            List<ColumnBinding> childBindings =
                childBoundTable.boundColumns.get(joinColumn.getChild());
            List<ColumnBinding> parentBindings =
                parentBoundTable.boundColumns.get(joinColumn.getParent());
            if ((childBindings == null) || (parentBindings == null))
                return null;
            ValueNode matchingEquals = null;
            found:
            for (ColumnBinding childBinding : childBindings) {
                ValueNode equals = childBoundTable.joinedColumns.get(childBinding);
                for (ColumnBinding parentBinding : parentBindings) {
                    if (equals == parentBoundTable.joinedColumns.get(parentBinding)) {
                        matchingEquals = equals;
                        break found;
                    }
                }
            }
            if (matchingEquals == null)
                return null;
            if (result == null)
                result = new HashSet<ValueNode>(1);
            result.add(matchingEquals);
        }
        return result;
    }

    /* Group rewriting */

    protected QueryTreeNode rewriteNode(QueryTreeNode node) throws StandardException {
        switch (node.getNodeType()) {
        case NodeTypes.FROM_LIST:
            return rewriteFromList((FromList)node);
        case NodeTypes.AND_NODE:
            return rewriteAndNode((AndNode)node);
        case NodeTypes.COLUMN_REFERENCE:
            return rewriteColumnReference((ColumnReference)node);
        default:
            break;
        }
        return node;
    }

    protected FromList rewriteFromList(FromList fromList) throws StandardException {
        Map<GroupBinding,FromBaseTable> groupTables = 
            new HashMap<GroupBinding,FromBaseTable>();
        boolean[] bholder = new boolean[1];
        for (int i = 0; i < fromList.size(); i++) {
            FromTable fromTable = fromList.get(i);
            FromTable groupTable = null;
            boolean newGroup = false;
            if (fromTable instanceof JoinNode) {
                // TODO: Is there a way to make the two cases more parallel?
                groupTable = joinGroupTable((JoinNode)fromTable, groupTables);
                if (groupTable == fromTable)
                    groupTable = null;
                else {
                    newGroup = true;
                    for (int j = 0; j < i; j++) {
                        if (fromList.get(j) == groupTable) {
                            newGroup = false;
                            break;
                        }
                    }
                }
            }
            else {
                bholder[0] = false;
                groupTable = getGroupTable(fromTable, groupTables, bholder);
                newGroup = bholder[0];
            }
            if (groupTable == null) {
                // Leave original node there.
            }
            else if (newGroup) {
                // First time this group used.
                fromList.set(i, groupTable);
            }
            else {
                // Duplicate group binding.
                fromList.remove(i);
                i--;
            }
        }
        return fromList;
    }

    protected FromBaseTable getGroupTable(FromTable fromTable, 
                                          Map<GroupBinding,FromBaseTable> groupTables,
                                          boolean[] return_newGroup) 
            throws StandardException {
        BoundTable boundTable = allBoundTables.get(fromTable);
        if (boundTable == null)
            return null;

        GroupBinding groupBinding = boundTable.groupBinding;
        if (groupBinding == null)
            return null;                            // Negative case is those without a group.

        FromBaseTable groupTable = groupTables.get(groupBinding);
        if (groupTable != null)
            return groupTable;

        GroupTable aisGroupTable = groupBinding.getGroup().getGroupTable();
        com.akiban.ais.model.TableName aisTableName = aisGroupTable.getName();
        TableName groupTableName = (TableName)
            nodeFactory.getNode(NodeTypes.TABLE_NAME,
                                aisTableName.getSchemaName(),
                                aisTableName.getTableName(),
                                parserContext);
        groupTable = (FromBaseTable)
            nodeFactory.getNode(NodeTypes.FROM_BASE_TABLE,
                                groupTableName,
                                groupBinding.getCorrelationName(),
                                null, null,
                                parserContext);
        TableBinding groupTableBinding = new TableBinding(aisGroupTable, false);
        groupTableBinding.setGroupBinding(groupBinding);
        groupTable.setUserData(groupTableBinding);
        groupTables.put(groupBinding, groupTable);
        if (return_newGroup != null)
            return_newGroup[0] = true;
        return groupTable;
    }

    protected FromTable joinGroupTable(JoinNode joinNode, 
                                       Map<GroupBinding,FromBaseTable> groupTables)
            throws StandardException {
        FromTable leftJoin = (FromTable)joinNode.getLeftResultSet();
        FromTable rightJoin = (FromTable)joinNode.getRightResultSet();
        if (leftJoin instanceof JoinNode)
            leftJoin = joinGroupTable((JoinNode)leftJoin, groupTables);
        else {
            FromTable groupTable = getGroupTable(leftJoin, groupTables, null);
            if (groupTable != null) {
                leftJoin = groupTable;
                joinNode.setLeftResultSet(leftJoin);
            }
        }
        if (rightJoin instanceof JoinNode)
            rightJoin = joinGroupTable((JoinNode)rightJoin, groupTables);
        else {
            FromTable groupTable = getGroupTable(rightJoin, groupTables, null);
            if (groupTable != null) {
                rightJoin = groupTable;
                joinNode.setRightResultSet(rightJoin);
            }
        }
        if (leftJoin == rightJoin)
            // This is the normal group case: both sides use the same group table.  
            // TODO: We lose the INNER / OUTER distinction here;
            // need to annotate group binding.
            return leftJoin;
        else
            // Did not coalesce top. (May have brought in groups below, though).
            return joinNode;
    }

    protected AndNode rewriteAndNode(AndNode andNode) throws StandardException {
        if (allJoinConditions.contains(andNode.getLeftOperand())) {
            andNode.setLeftOperand((BooleanConstantNode)
                                   nodeFactory.getNode(NodeTypes.BOOLEAN_CONSTANT_NODE,
                                                       Boolean.TRUE,
                                                       parserContext));
        }
        return andNode;
    }

    protected ColumnReference rewriteColumnReference(ColumnReference columnReference) 
            throws StandardException {
        ColumnBinding columnBinding = (ColumnBinding)columnReference.getUserData();
        if (columnBinding != null) {
            BoundTable boundTable = allBoundTables.get(columnBinding.getFromTable());
            if (boundTable != null) {
                GroupBinding groupBinding = boundTable.groupBinding;
                if (groupBinding != null) {
                    TableName groupTableName = (TableName)
                        nodeFactory.getNode(NodeTypes.TABLE_NAME,
                                            null,
                                            groupBinding.getCorrelationName(),
                                            parserContext);
                    ColumnReference groupColumnReference = (ColumnReference)
                        nodeFactory.getNode(NodeTypes.COLUMN_REFERENCE,
                                            columnBinding.getColumn().getGroupColumn().getName(),
                                            groupTableName,
                                            parserContext);
                    groupColumnReference.setType(columnReference.getType());
                    return groupColumnReference;
                }
            }
        }
        return columnReference;
    }

    /* Visitor interface */

    public Visitable visit(Visitable node) throws StandardException {
        switch (visitMode) {
        case GROUP:
            switch (((QueryTreeNode)node).getNodeType()) {
            case NodeTypes.SELECT_NODE:
                groupSelectNode((SelectNode)node);
                break;
            }
            break;
        case REWRITE:
            return rewriteNode((QueryTreeNode)node);
        default:
            assert false : "Invalid visit mode";
        }
        return node;
    }

    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }
    public boolean stopTraversal() {
        return false;
    }
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }

}
