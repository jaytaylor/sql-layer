/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
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
public class BranchJoiner extends BaseRule 
{
    private static final Logger logger = LoggerFactory.getLogger(BranchJoiner.class);

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
        else if (scan instanceof GroupLoopScan) {
            requiredTables = ((GroupLoopScan)scan).getRequiredTables();
        }
        markBranches(tableGroup, requiredTables);
        top:
        if (scan instanceof IndexScan) {
            IndexScan indexScan = (IndexScan)scan;
            TableSource indexTable = indexScan.getLeafMostTable();
            TableGroupJoinNode leafTable = rootTable.findTable(indexTable);
            assert (leafTable != null) : indexScan;
            List<TableSource> ancestors = new ArrayList<TableSource>();
            pendingTableSources(leafTable, rootTable, ancestors);
            if (isParent(leafTable)) {
                if (ancestors.remove(indexTable))
                    setPending(leafTable); // Changed from ancestor to branch.
                List<TableSource> tables = new ArrayList<TableSource>();
                scan = new BranchLookup(scan, indexTable.getTable(), tables);
                leafTable = singleBranchPending(leafTable, tables);
            }
            else if (!isRequired(leafTable)) {
                // Don't need the table that the index points to or
                // anything beneath it; might be able to jump to a
                // side branch.
                PlanNode sideScan = trySideBranch(scan, leafTable, rootTable,
                                                  indexTable, ancestors);
                if (sideScan != null) {
                    scan = sideScan;
                    break top;
                }
            }
            if (!ancestors.isEmpty())
                scan = new AncestorLookup(scan, indexTable, ancestors);
            scan = flatten(scan, leafTable, rootTable);
            scan = fillSideBranches(scan, leafTable, rootTable);
        }
        else if (scan instanceof GroupScan) {
            GroupScan groupScan = (GroupScan)scan;
            List<TableSource> tables = new ArrayList<TableSource>();
            groupScan.setTables(tables);
            scan = fillBranch(scan, tables, rootTable, rootTable, rootTable);
        }
        else if (scan instanceof GroupLoopScan) {
            GroupLoopScan groupLoop = (GroupLoopScan)scan;
            TableSource outsideTable = groupLoop.getOutsideTable();
            TableSource insideTable = groupLoop.getInsideTable();
            if (groupLoop.isInsideParent()) {
                TableGroupJoinNode parent = rootTable.findTable(groupLoop.getInsideTable());
                assert (parent != null) : groupLoop;
                List<TableSource> ancestors = new ArrayList<TableSource>();
                pendingTableSources(parent, rootTable, ancestors);
                scan = new AncestorLookup(scan, outsideTable, ancestors);
                scan = flatten(scan, parent, rootTable);
                scan = fillGroupLoopBranches(scan, parent, rootTable);
            }
            else {
                assert (groupLoop.getInsideTable() == rootTable.getTable());
                List<TableSource> tables = new ArrayList<TableSource>();
                scan = new BranchLookup(scan, outsideTable.getTable(), insideTable.getTable(), tables);
                scan = fillBranch(scan, tables, rootTable, rootTable, rootTable);
            }
        }
        else {
            throw new AkibanInternalException("Unknown TableGroupJoinTree scan");
        }
        for (TableGroupJoinNode table : tableGroup) {
            assert !isPending(table) : table;
        }
        return scan;
    }

    /** Try to switch from the main branch over to a side branch.
     * When there is nothing on the main branch, it would work to just
     * carry on and <code>Product</code> the <code>IndexScan</code>
     * alone with whatever else is needed. But a slightly better plan
     * is to switch over to a some needed branch with a non-nested
     * <code>BranchLookup</code> and carry on from there.
     */
    protected PlanNode trySideBranch(PlanNode scan,
                                     TableGroupJoinNode leafTable,
                                     TableGroupJoinNode rootTable,
                                     TableSource indexTable,
                                     List<TableSource> ancestors) {
        // If there any any ancestors, need a child of the leaf-most,
        // so that we can BranchLookup over there and still be able to
        // get all the required ancestors. If there aren't any
        // ancestors, anyplace that BranchLookup can take us to will
        // do.
        boolean findRequired = !ancestors.isEmpty();
        TableGroupJoinNode leafMostChild = leafTable;
        TableGroupJoinNode leafMostParent = null;
        while (leafMostChild != rootTable) {
            TableGroupJoinNode parent = leafMostChild.getParent();
            if (findRequired ? isRequired(parent) : isParent(parent)) {
                leafMostParent = parent;
                break;
            }
            leafMostChild = parent;
        }
        TableGroupJoinNode sideBranch = null;
        if (leafMostParent != null) {
            TableGroupJoinNode childParent = null;
            // Is some child of the leaf-most ancestor required or a
            // parent? If so, there is something beneath it, so it's a
            // good choice.
            for (TableGroupJoinNode table = leafMostParent.getFirstChild(); table != null; table = table.getNextSibling()) {
                if (isRequired(table)) {
                    sideBranch = table;
                    break;
                }
                if (isParent(table)) {
                    childParent = table;
                }
            }
            if (sideBranch == null)
                sideBranch = childParent;
        }
        if (sideBranch == null)
            return null;
        List<TableSource> tables = new ArrayList<TableSource>();
        // If both AncestorLookup and BranchLookup are needed, the
        // same index row cannot be used for both, as such a
        // heterogeneous rowtype stream is not allowed.
        // TODO: BranchLookup first is only superior when there are
        // relatively fewer of the side branch (perhaps even none);
        // otherwise the AncestorLookup is repeated for each instead
        // of once. Also, if the flattening is an outer join to the
        // side branch, it needs to come second.
        if (!ancestors.isEmpty() ||
            // Also, jumping to the same rowtype won't just work; must
            // go up to ancestor first.
            (leafMostChild.getTable().getTable() == sideBranch.getTable().getTable())) {
            if (!ancestors.contains(leafMostParent.getTable()))
                ancestors.add(leafMostParent.getTable());
            scan = new AncestorLookup(scan, indexTable, ancestors);
            scan = new BranchLookup(scan,
                                    leafMostParent.getTable().getTable(),
                                    sideBranch.getTable().getTable(), tables);
        }
        else {
            // Otherwise, it's better to the the BranchLookup first in
            // case don't need immediate ancestor but do need others.
            scan = new BranchLookup(scan, indexTable.getTable(), 
                                    leafMostParent.getTable().getTable(),
                                    sideBranch.getTable().getTable(), tables);
            if (!ancestors.isEmpty())
                // Any ancestors of indexTable are also ancestors of sideBranch.
                scan = new AncestorLookup(scan, sideBranch.getTable(), ancestors);
        }
        // And flatten up through root-most ancestor.
        return fillBranch(scan, tables, sideBranch, rootTable, rootTable);
    }

    /** Given a <code>BranchLookup</code> / <code>GroupScan</code>,
     * pick a primary branch under <code>underRoot</code>, flatten it
     * up to <code>flattenRoot</code> onto <code>input</code> and then
     * <code>Product</code> that with any remaining branches up to
     * <code>sideRoot</code>.
     */
    protected PlanNode fillBranch(PlanNode input, List<TableSource> lookupTables,
                                  TableGroupJoinNode underRoot, 
                                  TableGroupJoinNode flattenRoot,
                                  TableGroupJoinNode sideRoot) {
        TableGroupJoinNode leafTable = singleBranchPending(underRoot, lookupTables);
        return fillSideBranches(flatten(input, leafTable, flattenRoot),
                                leafTable, sideRoot);
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
        JoinType joinType = null;
        ConditionList joinConditions = new ConditionList(0);
        TableGroupJoinNode table = leafTable;
        while (true) {
            if (isRequired(table)) {
                assert !isPending(table);
                if (joinType != null) 
                    joinTypes.add(joinType);
                tableSources.add(table.getTable());
                tableNodes.add(table.getTable().getTable());
                if (table != rootTable) {
                    joinType = table.getParentJoinType();
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
        TableGroupJoinNode branchTable = leafTable;
        while (branchTable != rootTable) {
            TableGroupJoinNode parent = branchTable.getParent();
            if (isBranchpoint(parent)) {
                List<PlanNode> subplans = new ArrayList<PlanNode>(2);
                subplans.add(input);
                for (TableGroupJoinNode sibling = parent.getFirstChild();
                     sibling != null; sibling = sibling.getNextSibling()) {
                    if ((sibling == branchTable) ||
                        (leafLeftMostPending(sibling) == null))
                        continue;
                    List<TableSource> tables = new ArrayList<TableSource>();
                    PlanNode subplan = new BranchLookup(null, // no input means _Nested.
                                                        parent.getTable().getTable(),
                                                        sibling.getTable().getTable(), 
                                                        tables);
                    subplan = fillBranch(subplan, tables, sibling, parent, sibling);
                    if (subplans == null)
                        subplans = new ArrayList<PlanNode>();
                    subplans.add(subplan);
                }
                if (subplans.size() > 1)
                    input = new Product(parent.getTable().getTable(), subplans);
            }
            branchTable = parent;
        }
        return input;
    }

    /** Given ancestors from <code>parentTable</code> through
     * <code>rootTable</code> whose child is in another group tree,
     * fill out branches.
     */
    protected PlanNode fillGroupLoopBranches(PlanNode input, 
                                             TableGroupJoinNode parentTable,
                                             TableGroupJoinNode rootTable) {
        TableGroupJoinNode leafTable = parentTable;
        if (isParent(parentTable)) {
            // Also has children within the group tree. Take one for
            // in-stream branch.
            leafTable = parentTable.getFirstChild();
            List<TableSource> tables = new ArrayList<TableSource>();
            input = new BranchLookup(input,
                                     parentTable.getTable().getTable(),
                                     leafTable.getTable().getTable(), 
                                     tables);
            input = fillBranch(input, tables, leafTable, parentTable, leafTable);
        }
        return fillSideBranches(input, leafTable, rootTable);
    }
    
    /** Pick a branch beneath <code>rootTable</code> that is pending,
     * gather it into <code>tableSources</code> and return its leaf.
     */
    protected TableGroupJoinNode singleBranchPending(TableGroupJoinNode rootTable,
                                                     List<TableSource> tableSources) {
        TableGroupJoinNode leafTable = leafLeftMostPending(rootTable);
        assert (leafTable != null);
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

    /** This table needs to be included in flattens, either because
     * its columns are needed or it is a source for a
     * <code>BranchLookup</code>. */
    protected static final long REQUIRED = 1;
    /** This table has at least one descendants. */
    protected static final long PARENT = 2;
    /** This table has at least <em>two</em> active descendants, which
     * means that it is where two branches meet. */
    protected static final long BRANCHPOINT = 4;
    /** This table has not yet been included in result plan nodes. */
    protected static final long PENDING = 8;

    protected static boolean isRequired(TableGroupJoinNode table) {
        return ((table.getState() & REQUIRED) != 0);
    }
    protected static boolean isParent(TableGroupJoinNode table) {
        return ((table.getState() & PARENT) != 0);
    }
    protected static boolean isBranchpoint(TableGroupJoinNode table) {
        return ((table.getState() & BRANCHPOINT) != 0);
    }
    protected static boolean isPending(TableGroupJoinNode table) {
        return ((table.getState() & PENDING) != 0);
    }
    protected static void setPending(TableGroupJoinNode table) {
        table.setState(table.getState() | PENDING);
    }
    protected static void clearPending(TableGroupJoinNode table) {
        table.setState(table.getState() & ~PENDING);
    }

    protected void markBranches(TableGroupJoinTree tableGroup, 
                                Set<TableSource> requiredTables) {
        markBranches(tableGroup.getRoot(), requiredTables);
    }

    private boolean markBranches(TableGroupJoinNode parent, 
                                 Set<TableSource> requiredTables) {
        long flags = 0;
        for (TableGroupJoinNode child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (markBranches(child, requiredTables)) {
                if ((flags & PARENT) == 0)
                    flags |= PARENT;
                else
                    flags |= BRANCHPOINT;
            }
        }
        if ((requiredTables == null) ||
            requiredTables.contains(parent.getTable()) ||
            ((flags & BRANCHPOINT) != 0)) {
            flags |= REQUIRED | PENDING;
        }
        parent.setState(flags);
        return (flags != 0);
    }

}
