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

package com.akiban.sql.optimizer.plan;

import com.akiban.ais.model.Column;
import com.akiban.ais.model.HKey;
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
    protected abstract boolean areEquivalent(Column one, Column two);
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
        private ConditionsCounter<C> conditionsCounter = new ConditionsCounter<C>(conditions.size());

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
            for (int outerI = 0, outerLen = previous.size(); outerI < outerLen; outerI++) {
                N outer = previous.get(outerI);
                outer.incrementConditionsCounter(conditionsCounter);
                int counted = conditionsCounter.conditionsCounted();
                // only try the leaves if the outer counted some conditions, but not all of them.
                if (counted > 0 && counted < conditionsCount) {
                    // at this point, "outer" satisfies some conditions, and more conditions are left
                    for (int leavesI = 0, leavesLen = leaves.size(); leavesI < leavesLen; leavesI++) {
                        N inner = leaves.get(leavesI);
                        if (inner == outer)
                            continue; // fast path, we know there's full overlap
                        inner.incrementConditionsCounter(conditionsCounter);
                        if (inner.isUseful(conditionsCounter) && outer.isUseful(conditionsCounter)) {
                            emit(outer, inner, current);
                        }
                        // if this isn't the last inner N, reset the counter to what it was at the top of the loop
                        if (leavesI < leavesLen) {
                            conditionsCounter.clear();
                            outer.incrementConditionsCounter(conditionsCounter);
                        }
                    }
                }
                if (outerI < outerLen) {
                    conditionsCounter.clear();
                }
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
        if (firstUTable != secondUTable && commonAncestor == firstUTable) {
            isMultiBranch = false;
            if (includesHKey(firstUTable, comparisonCols))
                output.add(intersect(second, first, comparisonsLen));
        }
        if (isMultiBranch) {
            Collection<Column> ancestorHKeys = ancestorHKeys(commonAncestor);
            // check if commonTrailing contains all ancestorHKeys, using equivalence
            for (Column hkeyCol : ancestorHKeys) {
                boolean found = false;
                for (Column commonCol : comparisonCols) {
                    if (areEquivalent(commonCol, hkeyCol)) {
                        found = true;
                        break;
                    }
                }
                if (!found)
                    return;
            }
            output.add(intersect(first, second, comparisonsLen));
        }
    }

    private Collection<Column> ancestorHKeys(UserTable ancefoo) {
        // find most common ancestor
        HKey hkey = ancefoo.hKey();
        int ncols = hkey.nColumns();
        List<Column> results = new ArrayList<Column>(ncols);
        for (int i=0; i < ncols; ++i) {
            results.add(hkey.column(i));
        }
        return results;
    }

    private boolean includesHKey(UserTable table, List<Column> columns) {
        HKey hkey = table.hKey();
        int ncols = hkey.nColumns();
        // no overhead, but O(N) per hkey segment. assuming ncols and columns is very small
        for (int i = 0; i < ncols; ++i) {
            if (!containsEquivalent(columns, hkey.column(i)))
                return false;
        }
        return true;
    }

    private boolean containsEquivalent(List<Column> cols, Column tgt) {
        for (Column col : cols) {
            if (areEquivalent(col, tgt))
                return true;
        }
        return false;
    }
}
