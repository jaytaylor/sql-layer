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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/** Move WHERE clauses closer to their table origin.
 * This rule runs after flattening has been laid out.
 *
 * Note: <i>prepone</i>, while not an American or British English
 * word, is the transparent opposite of <i>postpone</i>.
 */
// TODO: Something similar is needed to handle moving HAVING
// conditions on the group by fields across the aggregation boundary
// and WHERE conditions on subqueries (views) into the subquery
// itself. These need to run earlier to affect indexing. Not sure how
// to integrate all these. Maybe move everything earlier on and then
// recognize joins of such filtered tables as Joinable.
public class SelectPreponer extends BaseRule
{
    private static final Logger logger = LoggerFactory.getLogger(SelectPreponer.class);

    @Override
    protected Logger getLogger() {
        return logger;
    }

    static class TableOriginFinder implements PlanVisitor, ExpressionVisitor {
        List<PlanNode> origins = new ArrayList<PlanNode>();

        public void find(PlanNode root) {
            root.accept(this);
        }

        public List<PlanNode> getOrigins() {
            return origins;
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
            if (n instanceof IndexScan) {
                origins.add(n);
            }
            else if (n instanceof TableLoader) {
                if (n instanceof BasePlanWithInput) {
                    PlanNode input = ((BasePlanWithInput)n).getInput();
                    if (!((input instanceof TableLoader) ||
                          (input instanceof IndexScan))) {
                        // Will put input in, so don't bother putting both in.
                        origins.add(n);
                    }
                }
                else {
                    origins.add(n);
                }
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
    public void apply(PlanContext plan) {
        TableOriginFinder finder = new TableOriginFinder();
        finder.find(plan.getPlan());
        Preponer preponer = new Preponer();
        for (PlanNode origin : finder.getOrigins()) {
            preponer.addOrigin(origin);
        }
        preponer.moveDeferred();
    }
    
    // Holds the state of a single branch of a loop, which usually means a group.
    static class Branch {
        Map<TableSource,PlanNode> loaders;
        Map<ExpressionNode,PlanNode> indexColumns;
        List<PlanNode> joins;
        Map<PlanNode,Set<TableSource>> joined;
        
        public Branch() {
            loaders = new HashMap<TableSource,PlanNode>();
        }

        public void setIndex(IndexScan index) {
            indexColumns = new HashMap<ExpressionNode,PlanNode>();
            for (ExpressionNode column : index.getColumns()) {
                if (column != null) {
                    indexColumns.put(column, index);
                }
            }
        }

        public void addLoader(PlanNode loader) {
            for (TableSource table : ((TableLoader)loader).getTables()) {
                loaders.put(table, loader);
            }
        }

        // A within-group join: Flatten or Product.
        public void addJoin(PlanNode join) {
            if (joins == null)
                joins = new ArrayList<PlanNode>();
            joins.add(join);

            // Might be able to place multi-table conditions after a join.
            if (joined == null)
                joined = new HashMap<PlanNode,Set<TableSource>>();
            Set<TableSource> tables = new HashSet<TableSource>(loaders.keySet());
            joined.put(join, tables);
        }

        public void addFlatten(Flatten flatten) {
            // Limit to tables that are inner joined (and on the outer
            // side of outer joins.)
            Set<TableSource> inner = flatten.getInnerJoinedTables();
            loaders.keySet().retainAll(inner);
            if (indexColumns != null) {
                Iterator<ExpressionNode> iter = indexColumns.keySet().iterator();
                while (iter.hasNext()) {
                    ExpressionNode expr = iter.next();
                    if (expr.isColumn() && 
                        !inner.contains(((ColumnExpression)expr).getTable()))
                        iter.remove();
                }
            }
            addJoin(flatten);
        }

        public Branch merge(Branch other) {
            loaders.putAll(other.loaders);
            if (indexColumns == null)
                indexColumns = other.indexColumns;
            else if (other.indexColumns != null)
                indexColumns.putAll(other.indexColumns);
            if (joins == null)
                joins = other.joins;
            else if (other.joins != null)
                joins.addAll(other.joins);
            if (joined == null)
                joined = other.joined;
            else if (other.joined != null)
                joined.putAll(other.joined);
            return this;
        }

        public boolean isEmpty() {
            return ((joins == null) ||
                    (loaders.isEmpty() &&
                     ((indexColumns == null) || indexColumns.isEmpty())));
        }

        // Does this branch consist solely of an index?
        public boolean indexOnly() {
            return (loaders.isEmpty() && 
                    !((indexColumns == null) || indexColumns.isEmpty()));
        }
    }

    static class Preponer {
        Map<Product,Branch> products;
        Map<Select,SelectConditions> selects;
        
        public Preponer() {
        }

        public void addOrigin(PlanNode node) {
            Branch branch = new Branch();
            PlanNode prev = null;
            if (node instanceof IndexScan) {
                branch.setIndex((IndexScan)node);
                prev = node;
                node = node.getOutput();
            }
            while (node instanceof TableLoader) {
                branch.addLoader(node);
                prev = node;
                node = node.getOutput();
            }
            while (true) {
                if (node instanceof Flatten) {
                    // A Flatten takes a single stream of lookups.
                    branch.addFlatten((Flatten)node);
                }
                else if (node instanceof Product) {
                    // A Product takes multiple streams, so we may
                    // have seen this one before.  Always inner join
                    // as of now, so no filtering of sources.
                    Product product = (Product)node;
                    if (products == null)
                        products = new HashMap<Product,Branch>();
                    Branch obranch = products.get(product);
                    if (obranch != null)
                        branch = obranch.merge(branch);
                    else
                        branch.addJoin(node);
                }
                else
                    break;
                prev = node;
                node = node.getOutput();
            }
            boolean maps = false;
            while (node instanceof MapJoin) {
                switch (((MapJoin)node).getJoinType()) {
                case INNER:
                    break;
                case LEFT:
                case SEMI:
                    if (prev == ((MapJoin)node).getInner())
                        return;
                    break;
                default:
                    return;
                }
                maps = true;
                prev = node;
                node = node.getOutput();
            }
            if (node instanceof Select) {
                Select select = (Select)node;
                if (!maps) {
                    // No nested loops; can just move things now.
                    if (!branch.isEmpty()) {
                        // If we didn't see any joins, conditions will already
                        // follow loading directly -- nothing to move
                        // over.
                        new SelectConditions(select).moveConditions(branch);
                    }
                }
                else {
                    // Need to defer until have all the contributors
                    // to the Map joins.
                    if (selects == null)
                        selects = new HashMap<Select,SelectConditions>();
                    SelectConditions selectConditions = selects.get(select);
                    if (selectConditions == null) {
                        selectConditions = new SelectConditions(select);
                        selects.put(select, selectConditions);
                    }
                    selectConditions.addBranch(branch);
                }
            }
        }

        public void moveDeferred() {
            if (selects != null) {
                for (SelectConditions swm : selects.values()) {
                    swm.moveConditions(null);
                }
            }
        }

    }

    static class SelectConditions {
        Select select;
        ConditionDependencyAnalyzer dependencies;
        // The branches that are joined up to feed the Select, added in depth-first
        // order, meaning that tables from an earlier branch should be available as
        // bound variables to later ones.
        List<Branch> branches;

        public SelectConditions(Select select) {
            this.select = select;
            dependencies = new ConditionDependencyAnalyzer(select);
        }

        public void addBranch(Branch branch) {
            if (branches == null)
                branches = new ArrayList<Branch>();
            branches.add(branch);
        }

        
        // Have a straight path to these conditions and know where
        // tables came from.  See what can be moved back there.
        public void moveConditions(Branch branch) {
            assert ((branch != null) == (branches == null));
            Iterator<ConditionExpression> iter = select.getConditions().iterator();
            while (iter.hasNext()) {
                ConditionExpression condition = iter.next();
                ColumnSource singleTable = dependencies.analyze(condition);
                PlanNode moveTo = canMove(branch, singleTable);
                if ((moveTo != null) && (moveTo != select.getInput())) {
                    moveCondition(condition, moveTo);
                    iter.remove();
                }
            }
        }
        
        // Return where this condition can move.
        // TODO: Could move earlier after subset of joins by breaking apart Flatten.
        public PlanNode canMove(Branch branch, ColumnSource singleTable) {
            Set<TableSource> outerTables = null;
            if (branch == null) {
                // Several joined branches: find the shallowest one that has everything.
                // If the condition only references a single table, no
                // need to check outer bindings; it's wherever it is.
                if (singleTable == null)
                    outerTables = new HashSet<TableSource>();
                branch = findBranch(outerTables);
                if (branch == null)
                    return null;
            }
            if (branch.indexColumns != null) {
                // Can check the index column before it's used for lookup.
                PlanNode loader = getSingleIndexLoader(branch, outerTables);
                if (loader != null)
                    return loader;
            }
            Set<ColumnSource> allTables = dependencies.getReferencedTables();
            if ((singleTable == null) && (outerTables != null)) {
                // Might still narrow down to a single table within this branch.
                allTables.removeAll(outerTables);
                if (allTables.size() == 1)
                    singleTable = allTables.iterator().next();
            }
            if (singleTable != null)
                return branch.loaders.get(singleTable);
            if ((branch.joins != null) && !allTables.isEmpty()) {
                joins:
                for (PlanNode join : branch.joins) {
                    // Find the first (deepest) flatten that has all the tables we need.
                    Set<TableSource> tables = branch.joined.get(join);
                    for (ColumnSource table : allTables) {
                        if (!tables.contains(table))
                            continue joins;
                    }
                    return join;
                }
            }
            return null;
        }

        // Find the first branch that has enough to evaluate the condition.
        public Branch findBranch(Set<TableSource> outerTables) {
            for (Branch branch : branches) {
                if (branch.indexOnly()) {
                    // If the map branch is just an index, have to
                    // look at individual columns.
                    Set<TableSource> maybeOuterTables = null;
                    if (outerTables != null)
                        // Even though index only has some columns, can exclude whole
                        // tables for purposes of deeper branches.
                        maybeOuterTables = new HashSet<TableSource>();
                    boolean allFound = true;
                    for (ColumnExpression column : dependencies.getReferencedColumns()) {
                        if (branch.indexColumns.containsKey(column)) {
                            if (maybeOuterTables != null)
                                maybeOuterTables.add((TableSource)column.getTable());
                        }
                        else {
                            allFound = false;
                        }
                    }
                    if (allFound)
                        return branch;
                    if (maybeOuterTables != null)
                        outerTables.addAll(maybeOuterTables);
                }
                else {
                    boolean allFound = true;
                    for (ColumnSource referencedTable : dependencies.getReferencedTables()) {
                        if (outerTables != null) {
                            if (outerTables.contains(referencedTable))
                                continue;
                        }
                        if (!branch.loaders.containsKey(referencedTable)) {
                            allFound = false;
                            break;
                        }
                    }
                    if (allFound)
                        return branch;
                    if (outerTables != null)
                        // Not moving to this branch; its tables are then available.
                        outerTables.addAll(branch.loaders.keySet());
                }
            }
            return null;
        }

        // If all the referenced columns come from the same index, return it.
        public PlanNode getSingleIndexLoader(Branch branch,
                                             Set<TableSource> outerTables) {
            PlanNode single = null;
            for (ColumnExpression column : dependencies.getReferencedColumns()) {
                if (outerTables != null) {
                    if (outerTables.contains(column.getTable())) 
                        continue;
                }
                PlanNode loader = branch.indexColumns.get(column);
                if (loader == null)
                    return null;
                if (single == null)
                    single = loader;
                else if (single != loader)
                    return null;
            }
            return single;
        }

        // Move the given condition to a Select that is right after the given node.
        public void moveCondition(ConditionExpression condition, PlanNode before) {
            Select select = null;
            PlanWithInput after = before.getOutput();
            if (after instanceof Select)
                select = (Select)after;
            else {
                select = new Select(before, new ConditionList(1));
                after.replaceInput(before, select);
            }
            select.getConditions().add(condition);
        }

    }

}
