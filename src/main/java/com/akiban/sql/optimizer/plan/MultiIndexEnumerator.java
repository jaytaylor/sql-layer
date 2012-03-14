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
import com.akiban.ais.model.Index;
import com.akiban.ais.model.IndexColumn;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;
import com.akiban.sql.optimizer.rule.EquivalenceFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>An enumerator which, given an Index and a set of conditions, will produce a collection of
 * {@link MultiIndexCandidate} which represent valid multi-index intersection pairs.</p>
 * 
 * <p>Like {@link MultiIndexCandidate}, this class is generic and abstract; its only abstract method is one for
 * creating an empty {@link MultiIndexCandidate}. Also like that class, the expectation is that there will be two
 * subclasses for this class: one for unit testing, and one for production.</p>
 * @param <C> the condition type.
 */
public abstract class MultiIndexEnumerator<C,N extends IndexIntersectionNode> {
    
    Logger log = LoggerFactory.getLogger(MultiIndexEnumerator.class);
    
    protected abstract Column columnFromCondition(C condition);
    protected abstract N buildLeaf(MultiIndexCandidate<C> candidate);
    protected abstract N intersect(N first, N second, int comparisonCount);
    
    public Collection<N> get(Collection<? extends Index> indexes, Set<C> conditions,
                                             EquivalenceFinder<Column> columnEquivalences)
    {
        Map<Column,C> colsToConds = new HashMap<Column,C>(conditions.size());
        for (C cond : conditions) {
            Column column = columnFromCondition(cond);
            if (column == null) {
                log.warn("couldn't map <{}> to Column", cond);
                continue;
            }
            C old = colsToConds.put(column, cond);
            if (old != null) {
                handleDuplicateCondition(); // test hook
                return null;
            }
        }
        
        Set<N> results = new HashSet<N>();
        Map<Index,MultiIndexCandidate<C>> indexToCandidate = new HashMap<Index, MultiIndexCandidate<C>>(indexes.size());
        for (Index index : indexes) {
            MultiIndexCandidate<C> candidate = createCandidate(index, colsToConds);
            if (candidate.anyPegged())
                indexToCandidate.put(index, candidate);
        }

        // note: "inner" and "outer" here refer only to these loops, nothing more.
        for (Index outerIndex : indexes) {
            for (Index innerIndex : indexes) {
                if (outerIndex != innerIndex) {
                    MultiIndexCandidate<C> outerCandidate = indexToCandidate.get(outerIndex);
                    MultiIndexCandidate<C> innerCandidate = indexToCandidate.get(innerIndex);
                    if (outerCandidate != null && innerCandidate != null && !outerCandidate.equals(innerCandidate) ) {
                        N outerScan = buildLeaf(outerCandidate);
                        N indexScan = buildLeaf(innerCandidate);
                        emit(outerScan, indexScan, results, columnEquivalences);
                    }
                }
            }
        }
        return results;
    }

    protected void handleDuplicateCondition() {
    }

    private MultiIndexCandidate<C> createCandidate(Index index, Map<Column, C> colsToConds) {
        MultiIndexCandidate<C> result = new MultiIndexCandidate<C>(index);
        while(true) {
            Column nextCol = result.getNextFreeColumn();
            if (nextCol == null)
                break;
            C condition = colsToConds.get(nextCol);
            if (condition == null)
                break;
            result.peg(condition);
        }
        return result;
    }

    private void emit(N first, N second,
                      Collection<N> output, EquivalenceFinder<Column> columnEquivalences)
    {
        Table firstTable = first.getLeafMostUTable();
        Table secondTable = second.getLeafMostUTable();
            List<Column> commonTrailing = getCommonTrailing(first, second, columnEquivalences);
            UserTable firstUTable = (UserTable) firstTable;
            UserTable secondUTable = (UserTable) secondTable;
            // handle the two single-branch cases
            if (firstUTable.isDescendantOf(secondUTable)
                    && includesHKey(secondUTable, commonTrailing, columnEquivalences)) {
                output.add(intersect(first, second, commonTrailing.size()));
            }
            else if (secondUTable.isDescendantOf(firstUTable)
                    && includesHKey(firstUTable, commonTrailing, columnEquivalences)) {
                output.add(intersect(second, first, commonTrailing.size()));
            }
            else {
                // TODO -- enable when multi-branch is in
                // output.add(new MultiIndexPair(first, second));
                // output.add(new MultiIndexPair(second, first));
            }
    }

    private boolean includesHKey(UserTable table, List<Column> columns, EquivalenceFinder<Column> columnEquivalences) {
        HKey hkey = table.hKey();
        int ncols = hkey.nColumns();
        // no overhead, but O(N) per hkey segment. assuming ncols and columns is very small
        for (int i = 0; i < ncols; ++i) {
            if (!containsEquivalent(columns, hkey.column(i), columnEquivalences))
                return false;
        }
        return true;
    }

    private boolean containsEquivalent(List<Column> cols, Column tgt, EquivalenceFinder<Column> columnEquivalences) {
        for (Column listCol : cols) {
            if (columnEquivalences.areEquivalent(listCol, tgt))
                return true;
        }
        return false;
    }

    // TODO change all this to use List<IndexColumn>, so we can efficiently create sublists
    private List<Column> getCommonTrailing(N first, N second, EquivalenceFinder<Column> columnEquivalences)
    {
        List<Column> firstTrailing = orderingColumns(first);
        if (firstTrailing.isEmpty())
            return Collections.emptyList();
        List<Column> secondTrailing = orderingColumns(second);
        if (secondTrailing.isEmpty())
            return Collections.emptyList();
        
        int maxTrailing = Math.min(firstTrailing.size(), secondTrailing.size());
        List<Column> results = new ArrayList<Column>(maxTrailing);
        for (int i = 0; i < maxTrailing; ++i) {
            Column firstCol = firstTrailing.get(i);
            Column secondCol = secondTrailing.get(i);
            if (columnEquivalences.areEquivalent(firstCol, secondCol))
                results.add(firstCol);
            else
                break;
        }
        return results;
    }

    private List<Column> orderingColumns(N scan) {
        // TODO temporary; need to rework to use Index.getAllColumns eventually
        List<IndexColumn> allCols = scan.getOrderingColumns();
        
        int ncols=allCols.size();
        List<Column> results = new ArrayList<Column>(allCols.size() - ncols);
        for (int i = scan.getComparisonsCount(); i < ncols; ++i) {
            results.add(allCols.get(i).getColumn());
        }
        return results;
    }
}
