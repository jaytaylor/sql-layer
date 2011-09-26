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

import static com.akiban.sql.optimizer.rule.IndexPicker.TableJoinsFinder;

import com.akiban.sql.optimizer.plan.*;

import com.akiban.qp.operator.API.JoinType;

import com.akiban.server.error.UnsupportedSQLException;

import java.util.*;

/** Having sources table groups from indexes, get rows with XxxLookup
 * and join them together with Flatten, etc. */
public class BranchJoiner extends BaseRule 
{
    @Override
    public void apply(PlanContext planContext) {
        List<TableJoins> groups = new TableJoinsFinder().find(planContext.getPlan());
        for (TableJoins tableJoins : groups) {
            joinBranches(tableJoins);
        }
    }

    protected void joinBranches(TableJoins tableJoins) {
        PlanNode scan = tableJoins.getScan();
        IndexScan index = null;
        if (scan instanceof IndexScan) {
            index = (IndexScan)scan;
        }
        if (index == null) {
            scan = flattenBranch(scan, tableJoins.getJoins(), tableJoins.getTables());
        }
        else if (!index.isCovering()) {
            TableSource indexTable = index.getLeafMostTable();
            indexTable.getTable().getTree().colorBranches();
            long indexMask = indexTable.getTable().getBranches();
            List<TableSource> ancestors = new ArrayList<TableSource>();
            TableSource branch = null;
            for (TableSource table : index.getRequiredTables()) {
                long tableMask = table.getTable().getBranches();
                if ((indexMask & tableMask) == 0) {
                    // No common branches.
                    if ((branch == null) ||
                         ancestorOf(table, branch)) {
                        branch = table;
                    }
                    else if (!ancestorOf(branch, table)) {
                        throw new UnsupportedSQLException("Sibling branches: " + table + 
                                                          " and " + branch,
                                                          null);
                    }
                }
                else if ((table == indexTable) ||
                         ((indexMask != tableMask) ?
                          // Some different branches: one with more is higher up.
                          ((indexMask & tableMask) == indexMask) :
                          // Same branch: check depth.
                          (table.getTable().getDepth() <= indexTable.getTable().getDepth()))) {
                    // An ancestor.
                    ancestors.add(table);
                }
                else {
                    // A descendant.
                    if (branch == null)
                        branch = indexTable;
                    else
                        throw new UnsupportedSQLException("Sibling branches: " + table + 
                                                          " and " + branch,
                                                          null);
                }
            }
            if (branch == indexTable) {
                ancestors.remove(indexTable);
                scan = new BranchLookup(scan, indexTable, branch);
                branch = null;
            }
            if (!ancestors.isEmpty()) {
                Collections.sort(ancestors, tableSourceById);
                scan = new AncestorLookup(scan, indexTable, ancestors);
            }
            if (branch != null) {
                scan = new BranchLookup(scan, 
                                        (ancestors.isEmpty() ? 
                                         indexTable : 
                                         ancestors.get(0)), 
                                        branch);
            }
            Joinable requiredJoins = trimJoins(tableJoins.getJoins(), 
                                               index.getRequiredTables());
            if (requiredJoins != null)
                scan = new Flatten(scan, requiredJoins);
        }
        // TODO: Can now prepone some of the conditions before the flatten.
        // TODO: Better to keep the tableJoins and just replace the
        // inside? Do we need its state any more?
        tableJoins.getOutput().replaceInput(tableJoins, scan);
    }
    
    // Get rid of joins that are no longer needed because they were part of the index.
    protected Joinable trimJoins(Joinable joinable, Set<TableSource> requiredTables) {
        if (joinable instanceof TableSource) {
            if ((requiredTables == null) || requiredTables.contains(joinable))
                return joinable;
            else
                return null;
        }
        else if (joinable instanceof JoinNode) {
            JoinNode join = (JoinNode)joinable;
            removeGroupJoin(join);
            Joinable oleft = join.getLeft();
            Joinable oright = join.getRight();
            Joinable nleft = trimJoins(oleft, requiredTables);
            Joinable nright = trimJoins(oright, requiredTables);
            if ((oleft == nleft) && (oright == nright))
                return joinable;
            else if (nleft == null)
                return nright;
            else if (nright == null)
                return nleft;
            else {
                join.setLeft(nleft);
                join.setRight(nright);
                return join;
            }
        }
        else
            return joinable;
    }

    // We only want joins for their flattening pattern.
    // Make explain output more obvious by removing group join traces.
    // TODO: Also rejecting non-group joins; those could be supported with Select.
    protected void removeGroupJoin(JoinNode join) {
        List<ConditionExpression> conditions = join.getJoinConditions();
        int i = 0;
        while (i < conditions.size()) {
            ConditionExpression cond = conditions.get(i);
            if (cond.getImplementation() == 
                ConditionExpression.Implementation.GROUP_JOIN) {
                conditions.remove(i);
            }
            else {
                i++;
            }
        }
        if (!conditions.isEmpty())
            throw new UnsupportedSQLException("Non group join",
                                              conditions.get(0).getSQLsource());
        join.setGroupJoin(null);
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
        private Map<TableNode,Collection<TableSource>> sideBranches;

        // Initialize from the leaf of the main branch. Just sets up
        // the arrays and mask; does not actually add any tables.
        public Branching(TableNode leaf) {
            int size = leaf.getDepth() + 1;
            mainBranch = new TableNode[size];
            mainBranchSources = new TableSource[size];
            mainBranchMask = leaf.getBranches();
            sideBranches = new HashMap<TableNode,Collection<TableSource>>();
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

        public void addMainBranchTable(TableSource table) {
            int index = table.getTable().getDepth();
            mainBranchSources[index] = table;
            mainBranch[index] = table.getTable();
        }

        public void addSideBranchTable(TableSource table) {
            TableNode branchPoint = getBranchPoint(table.getTable());
            assert (branchPoint != null);
            Collection<TableSource> entry = sideBranches.get(branchPoint);
            if (entry == null) {
                entry = new HashSet<TableSource>();
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

        public Map<TableNode,Collection<TableSource>> getSideBranches() {
            return sideBranches;
        }

        // Return list of tables in the main branch, root to leaf.
        public List<TableNode> getMainBranchTableNodes() {
            List<TableNode> result = new ArrayList<TableNode>();
            for (int i = 0; i < mainBranch.length; i++) {
                if (mainBranch[i] != null)
                    result.add(mainBranch[i]);
            }
            return result;
        }

        // Return list of table sources in the same order.
        public List<TableSource> getMainBranchTableSources() {
            List<TableSource> result = new ArrayList<TableSource>();
            for (int i = 0; i < mainBranch.length; i++) {
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
    protected PlanNode flattenBranch(PlanNode input, Joinable joins, 
                                     Collection<TableSource> tables) {
        if (tables.isEmpty()) return input;
        Branching branching = new Branching(tables);
        for (TableSource table : tables) {
            branching.addTable(table);
        }
        if (branching.getNSideBranches() > 0)
            throw new UnsupportedSQLException("Too many branches", null);
        List<TableNode> flattenNodes = branching.getMainBranchTableNodes();
        List<TableSource> flattenSources = branching.getMainBranchTableSources();
        List<JoinType> joinTypes = 
            new ArrayList<JoinType>(Collections.nCopies(flattenSources.size() - 1,
                                                        JoinType.INNER_JOIN));
        List<ConditionExpression> joinConditions = new ArrayList<ConditionExpression>(0);
        copyJoins(joins, null, flattenSources, joinTypes, joinConditions);
        if (!joinConditions.isEmpty())
            input = new Filter(input, joinConditions);
        return new Flatten_New(input, flattenNodes, flattenSources, joinTypes);
    }

    // Turn a tree of joins into a regular flatten list.
    // This only works for the simple cases: LEFT joins down the
    // branch and no join conditions or only ones depending on the
    // optional table. Everything else needs to be done using a
    // general nested loop join.
    protected void copyJoins(Joinable joinable, JoinNode parent, 
                             List<TableSource> branch, 
                             List<JoinType> joinTypes,
                             List<ConditionExpression> joinConditions) {
        if (joinable.isTable()) {
            TableSource table = (TableSource)joinable;
            int idx = branch.indexOf(table);
            assert ((parent == null) ? (idx >= 0) : (idx > 0));
            if (parent != null) {
                joinTypes.set(idx - 1, parent.getJoinType());
                if (parent.getJoinConditions() != null) {
                    for (ConditionExpression cond : parent.getJoinConditions()) {
                        if (cond.getImplementation() !=
                            ConditionExpression.Implementation.GROUP_JOIN) {
                            if (!isSimpleJoinCondition(cond, table))
                                throw new UnsupportedSQLException("Join condition too complex", cond.getSQLsource());
                            joinConditions.add(cond);
                        }
                    }
                }
            }
        }
        else if (joinable.isJoin()) {
            JoinNode join = (JoinNode)joinable;
            switch (join.getJoinType()) {
            case INNER_JOIN:
            case LEFT_JOIN:
                break;
            default:
                throw new UnsupportedSQLException("Join too complex: " + join, null);
            }
            copyJoins(join.getLeft(), null, branch, joinTypes, joinConditions);
            copyJoins(join.getRight(), join, branch, joinTypes, joinConditions);
        }
    }

    // Is this join condition simple enough to execute before the join?
    protected boolean isSimpleJoinCondition(ConditionExpression cond,
                                            TableSource table) {
        if (!(cond instanceof ComparisonCondition))
            return false;
        ComparisonCondition comp = (ComparisonCondition)cond;
        ExpressionNode left = comp.getLeft();
        ExpressionNode right = comp.getRight();
        if (!(left.isColumn() &&
              (((ColumnExpression)left).getTable() == table)))
            return false;
        if (!((right instanceof ConstantExpression) || 
              (right instanceof ParameterExpression)))
            // TODO: Column from outer table okay, too. Need general predicate for that.
            return false;
        return true;
    }

    /** Is <code>t1</code> an ancestor of <code>t2</code>? */
    protected boolean ancestorOf(TableSource t1, TableSource t2) {
        do {
            if (t1 == t2) return true;
            t2 = t2.getParentTable();
        } while (t2 != null);
        return false;
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
