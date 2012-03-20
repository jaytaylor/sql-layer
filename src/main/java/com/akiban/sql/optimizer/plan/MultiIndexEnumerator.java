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
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public abstract class MultiIndexEnumerator<C,N extends IndexIntersectionNode<C,N>> {

    protected abstract Collection<? extends C> getLeafConditions(N node);
    protected abstract N intersect(N first, N second, int comparisonCount);
    protected abstract boolean areEquivalent(Column one, Column two);
    protected abstract List<Column> getComparisonColumns(N first, N second);

    // becomes null when we start enumerating
    private List<N> results = new ArrayList<N>();
    private Set<C> conditions = new HashSet<C>();
    
    public void addLeaf(N leaf) {
        results.add(leaf);
    }
    
    public Collection<N> getCombinations() {
        if (results.isEmpty())
            return Collections.emptyList(); // return early if there's nothing here, cause why not.
        
        filterLeaves();
        
        final int leaves = results.size();
        
        List<N> freshNodes = results;
        List<N> oldNodes = results;
        List<N> newNodes = new ArrayList<N>(leaves);
        Set<C> conditionsCopy = new HashSet<C>(conditions);
        List<C> outerRecycle = new ArrayList<C>(conditions.size());
        List<C> innerRecycle = new ArrayList<C>(conditions.size());
        do {
            newNodes.clear();
            for (N outer : freshNodes) {
                if (outer.removeCoveredConditions(conditionsCopy, outerRecycle) && (!conditionsCopy.isEmpty())) {
                    for (N inner : oldNodes) {
                        if (inner != outer && inner.removeCoveredConditions(conditionsCopy, innerRecycle)) { // TODO if outer pegs [A] and inner pegs [A,B], this will emit, but it shouldn't.
                            emit(outer, inner, newNodes);
                            emptyInto(innerRecycle,conditionsCopy);
                        }
                    }
                }
                emptyInto(outerRecycle, conditionsCopy);
            }
            int oldCount = results.size();
            results.addAll(newNodes);
            oldNodes = results.subList(0, oldCount);
            freshNodes = results.subList(oldCount, results.size());
        } while (!newNodes.isEmpty());
        
        return results.subList(leaves, results.size());
    }

    private void filterLeaves() {
        for (Iterator<N> iter = results.iterator(); iter.hasNext(); ) {
            N leaf = iter.next();
            Collection<? extends C> nodeConditions = getLeafConditions(leaf);
            if ( (nodeConditions != null) && (!nodeConditions.isEmpty()) ) {
                conditions.addAll(nodeConditions);
            }
            else {
                iter.remove();
            }
        }
    }

    private static <T> void emptyInto(Collection<? extends T> source, Collection<? super T> target) {
        target.addAll(source);
        source.clear();
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
            else if (commonAncestor == secondUTable) {
                isMultiBranch = false;
                if (includesHKey(secondUTable, comparisonCols))
                    output.add(intersect(first, second, comparisonsLen));
            }
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
            output.add(intersect(second, first, comparisonsLen));
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
