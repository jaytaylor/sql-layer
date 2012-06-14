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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKeyColumn;
import com.akiban.ais.model.HKeySegment;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public abstract class MultiIndexEnumerator<C,N extends IndexIntersectionNode<C,N>,L extends N> implements Iterable<N> {

    protected abstract Collection<? extends C> getLeafConditions(L node);
    protected abstract N intersect(N first, N second, int comparisonCount);
    protected abstract List<Column> getComparisonColumns(N first, N second);

    // becomes null when we start enumerating
    private List<L> leaves = new ArrayList<L>();
    private Set<C> conditions = new HashSet<C>();
    
    public void addLeaf(L leaf) {
        leaves.add(leaf);
    }
    
    public Iterator<L> leavesIterator() {
        return leaves.iterator();
    }
    
    private class ComboIterator implements Iterator<N> {
        
        private boolean done = false;
        private List<N> current = new ArrayList<N>();
        private Iterator<N> currentIter;

        // These are only used in advancePhase, but we cache them to save on allocations
        private List<N> previous = new ArrayList<N>();
        private ConditionsCounter<C> outerCounter = new ConditionsCounter<C>(conditions.size());
        private ConditionsCounter<C> innerCounter = new ConditionsCounter<C>(conditions.size());
        private ConditionsCount<C> bothCount = new OverlayedConditionsCount<C>(outerCounter, innerCounter);

        private ComboIterator() {
            current.addAll(leaves);
            advancePhase();
        }

        @Override
        public boolean hasNext() {
            if (done)
                return false;
            if (currentIter.hasNext())
                return true;
            advancePhase();
            return !done;
        }

        @Override
        public N next() {
            if (done)
                throw new NoSuchElementException();
            if (!currentIter.hasNext())
                advancePhase();
            return currentIter.next();
        }

        @Override
        public void remove() {
            currentIter.remove();
        }

        private void advancePhase() {
            assert (currentIter == null) || (!currentIter.hasNext()) : "internal iterator not exhausted";
            if (current.isEmpty()) {
                done = true;
                return;
            }

            previous.clear();
            previous.addAll(current);
            current.clear();
            int conditionsCount = conditions.size();
            for (N outer : previous) {
                outer.incrementConditionsCounter(outerCounter);
                int counted = outerCounter.conditionsCounted();
                // only try the leaves if the outer counted some conditions, but not all of them.
                if (counted > 0 && counted < conditionsCount) {
                    // at this point, "outer" satisfies some conditions, and more conditions are left
                    for (L inner : leaves) {
                        if (inner == outer)
                            continue; // fast path, we know there's full overlap
                        inner.incrementConditionsCounter(innerCounter);
                        if (inner.isUseful(bothCount) && outer.isUseful(bothCount)) {
                            emit(outer, inner, current);
                        }
                        innerCounter.clear();
                    }
                }
                outerCounter.clear();
            }
            if (current.isEmpty()) {
                done = true;
                currentIter = null;
            }
            else {
                currentIter = current.iterator();
            }
        }
    }

    @Override
    public Iterator<N> iterator() {
        filterLeaves();
        return new ComboIterator();
    }

    private void filterLeaves() {
        for (Iterator<L> iter = leaves.iterator(); iter.hasNext(); ) {
            L leaf = iter.next();
            Collection<? extends C> nodeConditions = getLeafConditions(leaf);
            if ( (nodeConditions != null) && (!nodeConditions.isEmpty()) ) {
                conditions.addAll(nodeConditions);
            }
            else {
                iter.remove();
            }
        }
    }

    private void emit(N first, N second, Collection<N> output)
    {
        Table firstTable = first.getLeafMostUTable();
        Table secondTable = second.getLeafMostUTable();
        List<Column> comparisonCols = getComparisonColumns(first, second);
        if (comparisonCols.isEmpty())
            return;
        UserTable firstUTable = (UserTable) firstTable;
        UserTable secondUTable = (UserTable) secondTable;
        int comparisonsLen = comparisonCols.size();
        // find the UserTable associated with the common N. This works for multi- as well as single-branch
        UserTable commonAncestor = first.findCommonAncestor(second);
        assert commonAncestor == second.findCommonAncestor(first) : first + "'s ancestor not reflexive with " + second;
        boolean isMultiBranch = true;
        if (firstUTable != secondUTable) {
            if (commonAncestor == firstUTable) {
                isMultiBranch = false;
                if (includesHKey(firstUTable, comparisonCols))
                    output.add(intersect(second, first, comparisonsLen));
            }
            else {
                // in single-branch cases, we only want to output the leafmost's index
                isMultiBranch = (commonAncestor != secondUTable);
            }
        }
        if (isMultiBranch && includesHKey(commonAncestor, comparisonCols)) {
            output.add(intersect(first, second, comparisonsLen));
        }
    }

    private boolean includesHKey(UserTable table, List<Column> columns) {
        // TODO this seems horridly inefficient, but the data set is going to be quite small
        for (HKeySegment segment : table.hKey().segments()) {
            for (HKeyColumn hKeyCol : segment.columns()) {
                boolean found = false;
                for (Column equiv : hKeyCol.equivalentColumns()) {
                    if (columns.contains(equiv)) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return false;
            }
        }
        return true;
    }
}
