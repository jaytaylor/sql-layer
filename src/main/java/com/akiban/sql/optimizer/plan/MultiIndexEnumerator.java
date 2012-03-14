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
            List<IndexColumn> commonTrailing = getCommonTrailing(first, second, columnEquivalences);
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

    private boolean includesHKey(UserTable table, List<IndexColumn> columns, EquivalenceFinder<Column> columnEquivalences) {
        HKey hkey = table.hKey();
        int ncols = hkey.nColumns();
        // no overhead, but O(N) per hkey segment. assuming ncols and columns is very small
        for (int i = 0; i < ncols; ++i) {
            if (!containsEquivalent(columns, hkey.column(i), columnEquivalences))
                return false;
        }
        return true;
    }

    private boolean containsEquivalent(List<IndexColumn> cols, Column tgt, EquivalenceFinder<Column> columnEquivalences) {
        for (IndexColumn indexCol : cols) {
            Column col = indexCol.getColumn();
            if (columnEquivalences.areEquivalent(col, tgt))
                return true;
        }
        return false;
    }

    private List<IndexColumn> getCommonTrailing(N first, N second, EquivalenceFinder<Column> columnEquivalences)
    {
        List<IndexColumn> firstTrailing = orderingColumns(first);
        if (firstTrailing.isEmpty())
            return Collections.emptyList();
        List<IndexColumn> secondTrailing = orderingColumns(second);
        if (secondTrailing.isEmpty())
            return Collections.emptyList();
        
        int maxTrailing = Math.min(firstTrailing.size(), secondTrailing.size());
        int commonCount;
        for (commonCount = 0; commonCount < maxTrailing; ++commonCount) {
            Column firstCol = firstTrailing.get(commonCount).getColumn();
            Column secondCol = secondTrailing.get(commonCount).getColumn();
            if (!columnEquivalences.areEquivalent(firstCol, secondCol))
                break;
        }
        return firstTrailing.subList(0, commonCount);
    }

    private List<IndexColumn> orderingColumns(N scan) {
        List<IndexColumn> allCols = scan.getAllColumns();
        return allCols.subList(scan.getPeggedCount(), allCols.size());
    }
}
