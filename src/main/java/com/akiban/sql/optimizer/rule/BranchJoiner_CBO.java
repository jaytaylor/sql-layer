/**
 * Copyright (C) 2012 Akiban Technologies Inc.
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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;
import com.akiban.sql.optimizer.plan.TableGroupJoinTree.TableGroupJoinNode;

import com.akiban.server.error.AkibanInternalException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Get tables in groups into homogenous rows, consisting of Products
 * (of Products ...) of Flattened rows.
 * That is, the graph is covered by a connected set of "strips" of
 * flattened branches. Connectedness is achieved by
 * <code>KEEP_INPUT</code> and so replicating the branchpoint inside
 * the nested loop and then flattening it in.
 */
public class BranchJoiner_CBO extends BaseRule 
{
    private static final Logger logger = LoggerFactory.getLogger(BranchJoiner_CBO.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class TableGroupsFinder implements PlanVisitor, ExpressionVisitor {
        List<TableGroupJoinTree> result = new ArrayList<TableGroupJoinTree>();

        public List<TableGroupJoinTree> find(PlanNode root) {
            root.accept(this);
            return result;
        }

        @Override
        public boolean visitEnter(PlanNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(PlanNode n) {
            return true;
        }

        @Override
        public boolean visit(PlanNode n) {
            if (n instanceof TableGroupJoinTree) {
                result.add((TableGroupJoinTree)n);
            }
            return true;
        }

        @Override
        public boolean visitEnter(ExpressionNode n) {
            return visit(n);
        }

        @Override
        public boolean visitLeave(ExpressionNode n) {
            return true;
        }

        @Override
        public boolean visit(ExpressionNode n) {
            return true;
        }
    }

    @Override
    public void apply(PlanContext planContext) {
        List<TableGroupJoinTree> groups = new TableGroupsFinder().find(planContext.getPlan());
        for (TableGroupJoinTree tableGroup : groups) {
            PlanNode joins = joinBranches(tableGroup);
            tableGroup.getOutput().replaceInput(tableGroup, joins);
        }
    }

    protected PlanNode joinBranches(TableGroupJoinTree tableGroup) {
        TableGroupJoinNode rootTable = tableGroup.getRoot();
        PlanNode scan = tableGroup.getScan();
        Set<TableSource> requiredTables = null;
        if (scan instanceof IndexScan) {
            IndexScan indexScan = (IndexScan)scan;
            if (indexScan.isCovering())
                return indexScan;
            requiredTables = indexScan.getRequiredTables();
        }
        markBranches(tableGroup, requiredTables);
        if (scan instanceof IndexScan) {
            IndexScan indexScan = (IndexScan)scan;
            TableSource indexTable = indexScan.getLeafMostTable();
            TableGroupJoinNode leafTable = rootTable.findTable(indexTable);
            assert (leafTable != null) : indexScan;
            List<TableSource> ancestors = new ArrayList<TableSource>();
            pendingTableSources(leafTable, rootTable, ancestors);
            if (hasChildren(leafTable))
                ancestors.remove(indexTable);
            else if (ancestors.isEmpty()) {
                // No tables on the branch that the index points to.
                // It would work to just carry on and Product the
                // IndexScan with whatever else is needed. But a
                // slightly better plan is to switch over to a some
                // needed branch with a non-nested BranchLookup and
                // carry on from there.
                TableGroupJoinNode sideBranch = rootLeftMostRequired(rootTable);
                if (sideBranch == null)
                    return scan;
                List<TableSource> tables = new ArrayList<TableSource>();
                scan = new BranchLookup(scan, indexTable.getTable(), 
                                        sideBranch.getTable().getTable(), tables);
                return fillBranch(scan, tables, sideBranch);
            }
            if (!ancestors.isEmpty())
                scan = new AncestorLookup(scan, indexTable, ancestors);
            if (hasChildren(leafTable)) {
                List<TableSource> tables = new ArrayList<TableSource>();
                scan = new BranchLookup(scan, indexTable.getTable(), 
                                        indexTable.getTable(), tables);
                leafTable = singleBranchPending(leafTable, tables);
            }
            scan = flatten(scan, leafTable, rootTable);
            return fillSideBranches(scan, leafTable, rootTable);
        }
        else if (scan instanceof GroupScan) {
            GroupScan groupScan = (GroupScan)scan;
            return fillBranch(scan, groupScan.getTables(), rootTable);
        }
        else {
            throw new AkibanInternalException("Unknown TableGroupJoinTree scan");
        }
    }

    /** Given a <code>BranchLookup</code> / <code>GroupScan</code>,
     * pick a primary branch, flatten it onto <code>input</code> and
     * then <code>Product</code> that with any remaining branches.
     */
    protected PlanNode fillBranch(PlanNode input, List<TableSource> lookupTables,
                                  TableGroupJoinNode rootTable) {
        TableGroupJoinNode leafTable = singleBranchPending(rootTable, lookupTables);
        return fillSideBranches(flatten(input, leafTable, rootTable),
                                leafTable, rootTable);
    }

    /** Generate single branch <code>Flatten</code> joins from
     * <code>leafTable</code> to <code>rootTable</code>.
     */
    protected PlanNode flatten(PlanNode input, 
                               TableGroupJoinNode leafTable,
                               TableGroupJoinNode rootTable) {
        List<TableSource> tableSources = new ArrayList<TableSource>();
        List<TableNode> tableNodes = new ArrayList<TableNode>();
        List<JoinType> joinTypes = new ArrayList<JoinType>();
        ConditionList joinConditions = new ConditionList(0);
        TableGroupJoinNode table = leafTable;
        while (true) {
            if (isRequired(table)) {
                assert !isPending(table);
                tableSources.add(table.getTable());
                tableNodes.add(table.getTable().getTable());
                if (table != rootTable) {
                    joinTypes.add(table.getParentJoinType());
                    if (table.getJoinConditions() != null) {
                        for (ConditionExpression joinCondition : table.getJoinConditions()) {
                            if (joinCondition.getImplementation() != ConditionExpression.Implementation.GROUP_JOIN) {
                                joinConditions.add(joinCondition);
                            }
                        }
                    }
                }
            }
            if (table == rootTable) break;
            table = table.getParent();
        }
        Collections.reverse(tableSources);
        Collections.reverse(tableNodes);
        Collections.reverse(joinTypes);
        if (!joinConditions.isEmpty())
            input = new Select(input, joinConditions);
        return new Flatten(input, tableNodes, tableSources, joinTypes);
    }

    /** Given a flattened single branch from <code>leafTable</code> to
     * <code>rootTable</code>, <code>Product</code> in any additional
     * branches that are needed.
     */
    protected PlanNode fillSideBranches(PlanNode input, 
                                        TableGroupJoinNode leafTable,
                                        TableGroupJoinNode rootTable) {
        List<PlanNode> subplans = null;
        if (subplans == null)
            return input;
        TableGroupJoinNode branchTable = leafTable;
        while (true) {
            TableGroupJoinNode parent = branchTable.getParent();
            if (isBranchpoint(parent)) {
                for (TableGroupJoinNode sibling = parent.getFirstChild();
                     sibling != null; sibling = sibling.getNextSibling()) {
                    if (sibling == branchTable) continue;
                    List<TableSource> tables = new ArrayList<TableSource>();
                    PlanNode subplan = new BranchLookup(null, // no input means _Nested.
                                                        parent.getTable().getTable(),
                                                        sibling.getTable().getTable(), 
                                                        tables);
                    subplan = fillBranch(subplan, tables, sibling);
                    if (subplans == null)
                        subplans = new ArrayList<PlanNode>();
                    subplans.add(subplan);
                }
            }
            if (parent == rootTable) break;
            branchTable = parent;
        }
        subplans.add(input);
        Collections.reverse(subplans); // Root to leaf (sideways order doesn't matter).
        return new Product(subplans);
    }

    /** Pick a branch beneath <code>rootTable</code> that is pending,
     * gather it into <code>tableSources</code> and return its leaf.
     */
    protected TableGroupJoinNode singleBranchPending(TableGroupJoinNode rootTable,
                                                     List<TableSource> tableSources) {
        TableGroupJoinNode leafTable = leafLeftMostPending(rootTable);
        pendingTableSources(leafTable, rootTable, tableSources);
        return leafTable;
    }

    /** Get table sources marked as pending along the path from
     * <code>leafTable</code> up to <code>rootTable</code>, clearing
     * that flag along the way.
     */
    protected void pendingTableSources(TableGroupJoinNode leafTable,
                                       TableGroupJoinNode rootTable,
                                       List<TableSource> tableSources) {
        TableGroupJoinNode table = leafTable;
        while (true) {
            if (isPending(table)) {
                clearPending(table);
                tableSources.add(table.getTable());
            }
            if (table == rootTable) break;
            table = table.getParent();
        }
        Collections.reverse(tableSources); // Want root to leaf.
    }

    /** Find a pending leaf under the the given root. */
    protected TableGroupJoinNode leafLeftMostPending(TableGroupJoinNode rootTable) {
        TableGroupJoinNode leafTable = null;
        for (TableGroupJoinNode table : rootTable) {
            if ((leafTable != null) && !isAncestor(table, leafTable))
                break;
            if (isPending(table))
                leafTable = table;
        }
        return leafTable;
    }

    /** Find a required branch under the given root. */
    protected TableGroupJoinNode rootLeftMostRequired(TableGroupJoinNode rootTable) {
        for (TableGroupJoinNode table : rootTable) {
            if (isRequired(table))
                return table;
        }
        return null;
    }

    /** Is the given <code>rootTable</code> an ancestor of <code>leafTable</code>? */
    protected boolean isAncestor(TableGroupJoinNode leafTable,
                                 TableGroupJoinNode rootTable) {
        do {
            if (leafTable == rootTable)
                return true;
            leafTable = leafTable.getParent();
        } while (leafTable != null);
        return false;
    }

    /* Flags for TableGroupJoinNode */

    protected static final int REQUIRED = 1;
    protected static final int HAS_CHILDREN = 2;
    protected static final int BRANCHPOINT = 4;
    protected static final int PENDING = 8;

    protected static boolean isRequired(TableGroupJoinNode table) {
        return ((table.getFlags() & REQUIRED) != 0);
    }
    protected static boolean hasChildren(TableGroupJoinNode table) {
        return ((table.getFlags() & HAS_CHILDREN) != 0);
    }
    protected static boolean isBranchpoint(TableGroupJoinNode table) {
        return ((table.getFlags() & BRANCHPOINT) != 0);
    }
    protected static boolean isPending(TableGroupJoinNode table) {
        return ((table.getFlags() & PENDING) != 0);
    }
    protected static void clearPending(TableGroupJoinNode table) {
        table.setFlags(table.getFlags() & ~PENDING);
    }

    protected void markBranches(TableGroupJoinTree tableGroup, 
                                Set<TableSource> requiredTables) {
        markBranches(tableGroup.getRoot(), requiredTables);
    }

    private boolean markBranches(TableGroupJoinNode parent, 
                                 Set<TableSource> requiredTables) {
        int flags = 0;
        for (TableGroupJoinNode child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (markBranches(child, requiredTables)) {
                if ((flags & HAS_CHILDREN) == 0)
                    flags |= HAS_CHILDREN;
                else
                    flags |= BRANCHPOINT;
            }
        }
        if ((requiredTables == null) ||
            requiredTables.contains(parent.getTable()) ||
            ((flags & BRANCHPOINT) != 0)) {
            flags |= REQUIRED | PENDING;
        }
        parent.setFlags(flags);
        return (flags != 0);
    }

}
