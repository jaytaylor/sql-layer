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

package com.akiban.sql.optimizer.rule;

import com.akiban.sql.optimizer.plan.*;
import com.akiban.sql.optimizer.plan.JoinNode.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Having sources table groups from indexes, get rows with XxxLookup
 * and join them together with Flatten, Product, etc. */
public class BranchJoiner extends BaseRule 
{
    private static final Logger logger = LoggerFactory.getLogger(BranchJoiner.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class TableJoinsFinder implements PlanVisitor, ExpressionVisitor {
        List<TableJoins> result = new ArrayList<TableJoins>();

        public List<TableJoins> find(PlanNode root) {
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
            if (n instanceof TableJoins) {
                result.add((TableJoins)n);
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
        List<TableJoins> groups = new TableJoinsFinder().find(planContext.getPlan());
        for (TableJoins tableJoins : groups) {
            PlanNode joins = joinBranches(tableJoins);
            // TODO: Better to keep the tableJoins and just replace the
            // inside? Do we need its state any more?
            tableJoins.getOutput().replaceInput(tableJoins, joins);
        }
    }

    protected PlanNode joinBranches(TableJoins tableJoins) {
        PlanNode scan = tableJoins.getScan();
        IndexScan index = null;
        if (scan instanceof IndexScan) {
            index = (IndexScan)scan;
        }
        if (index == null) {
            Collection<TableSource> tables = tableJoins.getTables();
            if (scan instanceof GroupScan) {
                // Initial assumption is that all tables will make it
                // on, but this may be refined below by branching.
                List<TableSource> ntables = new ArrayList<TableSource>(tables);
                Collections.sort(ntables, tableSourceById);
                ((GroupScan)scan).setTables(ntables);
                tables = ntables;
            }
            return flattenBranches(scan, tableJoins.getJoins(), tables, null);
        }
        if (index.isCovering()) {
            return index;
        }
        // Partition tables by relationship with where the index points.
        TableSource indexTable = index.getLeafMostTable();
        TableNode indexTableNode = indexTable.getTable();
        indexTableNode.getTree().colorBranches();
        long indexMask = indexTableNode.getBranches();
        List<TableSource> ancestors = new ArrayList<TableSource>();
        List<TableSource> descendants = new ArrayList<TableSource>();
        List<TableSource> branched = new ArrayList<TableSource>();
        for (TableSource table : index.getRequiredTables()) {
            long tableMask = table.getTable().getBranches();
            if ((indexMask & tableMask) == 0) {
                // No common branches.
                branched.add(table);
            }
            else if ((table == indexTable) ||
                     ((indexMask != tableMask) ?
                      // Some different branches: one with more is higher up.
                      ((indexMask & tableMask) == indexMask) :
                      // Same branch: check depth.
                      (table.getTable().getDepth() <= indexTableNode.getDepth()))) {
                // An ancestor.
                ancestors.add(table);
            }
            else {
                // A descendant.
                descendants.add(table);
            }
        }
        if (descendants.isEmpty() && branched.isEmpty()) {
            if (ancestors.isEmpty()) 
                return scan;
            // Easy case 1: up a single branch from the index row. Look up and flatten.
            Collections.sort(ancestors, tableSourceById);
            AncestorLookup al = new AncestorLookup(scan, indexTable, ancestors);
            return flattenJoins(al, tableJoins.getJoins(),
                                al.getAncestors(), al.getTables());
        }
        Branching branching;
        // Fill in the main branch.
        if (descendants.isEmpty())
            branching = new Branching(indexTable);
        else
            branching = new Branching(descendants);
        for (TableSource ancestor : ancestors)
            branching.addMainBranchTable(ancestor);
        for (TableSource descendant : descendants)
            branching.addTable(descendant);
        // And the side branches.
        for (TableSource table : branched)
            branching.addTable(table);
        
        easy_2:
        if (descendants.isEmpty() && (branching.getNSideBranches() == 1)) {
            Map.Entry<TableNode,List<TableSource>> entry =
                branching.getSideBranches().entrySet().iterator().next();
            TableNode leafAncestor = indexTableNode;
            List<TableSource> allTables = entry.getValue();
            if (!ancestors.isEmpty()) {
                Collections.sort(ancestors, tableSourceById);
                leafAncestor = ancestors.get(ancestors.size()-1).getTable();
                long branchMask = entry.getKey().getBranches();
                if ((branchMask & leafAncestor.getBranches()) != branchMask)
                    break easy_2;
                scan = new AncestorLookup(scan, indexTable, ancestors);
                allTables = new ArrayList<TableSource>(allTables);
                allTables.addAll(ancestors);
            }
            // Easy case 2: just one side branch and either nothing on
            // the original branch or everything an ancestor of the
            // side branch (far enough above the index table).
            // Fetch it in the same stream.
            scan = new BranchLookup(scan, leafAncestor, 
                                    entry.getKey(), entry.getValue());
            return flattenBranches(scan, tableJoins.getJoins(), allTables, null);
        }

        // Load the main branch.
        List<TableNode> mainBranchNodes;
        List<TableSource> mainBranchSources;
        descendants.retainAll(branching.getMainBranchTableSources());
        if (descendants.isEmpty()) {
            mainBranchNodes = branching.getMainBranchTableNodes();
            mainBranchSources = branching.getMainBranchTableSources();
        }
        else {
            Collections.sort(descendants, tableSourceById);
            scan = new BranchLookup(scan, indexTableNode, 
                                    indexTableNode, descendants);
            // Only need the rest.
            mainBranchNodes = branching.getMainBranchTableNodesAbove(indexTableNode);
            mainBranchSources = branching.getMainBranchTableSourcesAbove(indexTableNode);
        }
        if (!mainBranchNodes.isEmpty()) {
            scan = new AncestorLookup(scan, indexTableNode, 
                                      mainBranchNodes, mainBranchSources);
        }
        return flattenBranches(scan, tableJoins.getJoins(), branching);
    }

    // A branch analysis consists of a single branch that is the basis
    // for a number of side branches.
    protected static class Branching {
        // The main branch: indexed by the depth of the table. Tables
        // can be in here because they are needed for the query or
        // because they are needed to branch off to a side branch.
        private TableNode[] mainBranch;
        // These are the query tables corresponding to that branch, in
        // the same order.
        private TableSource[] mainBranchSources;
        // The branch colors for the main branch. If a table has _all
        // these_, it is on that branch.
        private long mainBranchMask;
        // Each side branch and the tables that are reached that way.
        // The key is the brachpoint, an ancestor of the required
        // table whose ancestor is on the main branch.
        private Map<TableNode,List<TableSource>> sideBranches;

        // Initialize from the leaf of the main branch. Just sets up
        // the arrays and mask; does not actually add any tables.
        public Branching(TableNode leaf) {
            int size = leaf.getDepth() + 1;
            mainBranch = new TableNode[size];
            mainBranchSources = new TableSource[size];
            leaf.getTree().colorBranches();
            mainBranchMask = leaf.getBranches();
            sideBranches = new HashMap<TableNode,List<TableSource>>();
        }

        public Branching(TableSource leaf) {
            this(leaf.getTable());
        }

        // Initialize from a set of query tables, one of which is the leaf.
        public Branching(Collection<TableSource> tables) {
            this(leafTable(tables).getTable());
        }

        protected static TableSource leafTable(Collection<TableSource> tables) {
            TableSource leaf = null;
            for (TableSource table : tables) {
                if ((leaf == null) || (tableSourceById.compare(leaf, table) < 0))
                    leaf = table;
            }
            return leaf;
        }
        
        // Add in a table, which may be on the main branch or not.
        public boolean addTable(TableSource table) {
            if ((table.getTable().getBranches() & mainBranchMask) == mainBranchMask) {
                addMainBranchTable(table);
                return true;
            }
            else {
                addSideBranchTable(table);
                return false;
            }
        }

        public void addMainBranchTable(TableNode table) {
            int index = table.getDepth();
            mainBranch[index] = table;
        }

        public void addMainBranchTable(TableSource table) {
            int index = table.getTable().getDepth();
            mainBranchSources[index] = table;
            mainBranch[index] = table.getTable();
        }

        public void addSideBranchTable(TableSource table) {
            TableNode branchPoint = getBranchPoint(table.getTable());
            assert (branchPoint != null);
            addMainBranchTable(branchPoint.getParent());
            List<TableSource> entry = sideBranches.get(branchPoint);
            if (entry == null) {
                entry = new ArrayList<TableSource>();
                sideBranches.put(branchPoint, entry);
            }
            entry.add(table);
        }

        // Get an ancestor of the given table that has an ancestor on the main branch.
        protected TableNode getBranchPoint(TableNode table) {
            TableNode prev;
            do {
                prev = table;
                table = table.getParent();
                if ((table.getBranches() & mainBranchMask) == mainBranchMask)
                    return prev;
            } while (table != null);
            return null;
        }

        public int getNSideBranches() {
            return sideBranches.size();
        }

        public Map<TableNode,List<TableSource>> getSideBranches() {
            return sideBranches;
        }

        // Return list of tables in the main branch, root to leaf.
        public List<TableNode> getMainBranchTableNodes() {
            return getMainBranchTableNodes(mainBranch.length);
        }
            
        public List<TableNode> getMainBranchTableNodesAbove(TableNode limit) {
            return getMainBranchTableNodes(limit.getDepth());
        }

        public List<TableNode> getMainBranchTableNodes(int length) {
            List<TableNode> result = new ArrayList<TableNode>();
            for (int i = 0; i < length; i++) {
                if (mainBranch[i] != null)
                    result.add(mainBranch[i]);
            }
            return result;
        }

        // Return list of table sources in the same order.
        public List<TableSource> getMainBranchTableSources() {
            return getMainBranchTableSources(mainBranch.length);
        }

        public List<TableSource> getMainBranchTableSourcesAbove(TableNode limit) {
            return getMainBranchTableSources(limit.getDepth());
        }

        public List<TableSource> getMainBranchTableSources(int length) {
            List<TableSource> result = new ArrayList<TableSource>();
            for (int i = 0; i < length; i++) {
                if (mainBranch[i] != null)
                    result.add(mainBranchSources[i]);
            }
            return result;
        }
    }

    // Given the result of a BranchLookup / GroupScan, flatten completely.
    // Even though all the rows are there, without a
    // Product_HKeyOrdered kind of operator, it is may not be possible
    // to do this without fetching some of the data over again.
    protected PlanNode flattenBranches(PlanNode input, Joinable joins, 
                                       Collection<TableSource> tables,
                                       TableNode commonAncestor) {
        if (tables.isEmpty()) 
            return input;

        Branching branching = new Branching(tables);
        for (TableSource table : tables) {
            branching.addTable(table);
        }
        if (commonAncestor != null)
            branching.addMainBranchTable(commonAncestor);
        // Any tables that need to be moved to a side branch didn't
        // come from the initial tree after all.
        tables.retainAll(branching.getMainBranchTableSources());
        return flattenBranches(input, joins, branching);
    }

    protected PlanNode flattenBranches(PlanNode input, Joinable joins,
                                       Branching branching) {
        List<TableNode> flattenNodes = branching.getMainBranchTableNodes();
        List<TableSource> flattenSources = branching.getMainBranchTableSources();

        // Flatten the main branch.
        input = flattenJoins(input, joins, flattenNodes, flattenSources);
        
        int nbranches = branching.getNSideBranches();
        if (nbranches > 0) {
            // Need a product of several branches.
            List<PlanNode> subplans = new ArrayList<PlanNode>(nbranches + 1);
            subplans.add(input);

            List<TableNode> branchpoints = 
                new ArrayList<TableNode>(branching.getSideBranches().keySet());
            Collections.sort(branchpoints, tableNodeById);
            for (TableNode branchpoint : branchpoints) {
                List<TableSource> subbranch = 
                    branching.getSideBranches().get(branchpoint);
                Collections.sort(subbranch, tableSourceById);
                PlanNode subplan = new BranchLookup(null, // No input.
                                                    branchpoint.getParent(),
                                                    branchpoint,
                                                    subbranch);
                // Try to flatten just this side branch, maybe giving nested product.
                subplan = flattenBranches(subplan, joins, 
                                          subbranch, branchpoint.getParent());
                subplans.add(subplan);
            }

            input = new Product(subplans);
        }
        return input;
    }

    // Given tables that need to be flattened, account for join types
    // and conditions and generate the actual Flatten node.
    protected PlanNode flattenJoins(PlanNode input, Joinable joins,
                                    List<TableNode> flattenNodes,
                                    List<TableSource> flattenSources) {
        List<JoinType> joinTypes = 
            new ArrayList<JoinType>(Collections.nCopies(flattenSources.size() - 1,
                                                        JoinType.INNER));
        ConditionList joinConditions = new ConditionList(0);
        copyJoins(joins, flattenSources, joinTypes, joinConditions);
        if (!joinConditions.isEmpty())
            input = new Select(input, joinConditions);
        return new Flatten(input, flattenNodes, flattenSources, joinTypes);
    }

    // Turn a tree of joins into a regular flatten list.  This only
    // works for the simple cases: RIGHT then INNER then LEFT joins
    // down the branch and no join conditions or only ones depending
    // on the optional table. Everything else needs to be done using a
    // general nested loop join.
    protected void copyJoins(Joinable joinable,
                             List<TableSource> branch, 
                             List<JoinType> joinTypes,
                             ConditionList joinConditions) {
        if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            if (join.hasJoinConditions()) {
                for (ConditionExpression cond : join.getJoinConditions()) {
                    if (cond.getImplementation() !=
                        ConditionExpression.Implementation.GROUP_JOIN) {
                        if (!isSimpleJoinCondition(cond, join))
                            throw new UnsupportedSQLException("Join condition too complex", cond.getSQLsource());
                        joinConditions.add(cond);
                    }
                }
            }
            JoinType joinType = join.getJoinType();
            if (joinType != JoinType.INNER) {
                boolean found = false;
                if (join.getLeft().isTable()) {
                    TableSource table = (TableSource)join.getLeft();
                    int idx = branch.indexOf(table);
                    if (idx >= 0) {
                        joinTypes.set(idx, joinType);
                        found = true;
                    }
                }
                if (!found && join.getRight().isTable()) {
                    TableSource table = (TableSource)join.getRight();
                    int idx = branch.indexOf(table);
                    if (idx > 0) {
                        joinTypes.set(idx - 1, joinType);
                        found = true;
                    }
                }
            }
            copyJoins(join.getLeft(), branch, joinTypes, joinConditions);
            copyJoins(join.getRight(), branch, joinTypes, joinConditions);
        }
    }

    // Is this join condition simple enough to execute before the join?
    protected boolean isSimpleJoinCondition(ConditionExpression cond,
                                            JoinNode join) {
        TableSource table = SelectPreponer.getSingleTableConditionTable(cond);
        return ((table == join.getLeft()) || (table == join.getRight()));
    }

    static final Comparator<TableSource> tableSourceById = new Comparator<TableSource>() {
        @Override
        // Access things in stable order.
        public int compare(TableSource t1, TableSource t2) {
            return t1.getTable().getTable().getTableId().compareTo(t2.getTable().getTable().getTableId());
        }
    };

    static final Comparator<TableNode> tableNodeById = new Comparator<TableNode>() {
        @Override
        public int compare(TableNode t1, TableNode t2) {
            return t1.getTable().getTableId().compareTo(t2.getTable().getTableId());
        }
    };

}
