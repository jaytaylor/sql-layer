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
 * 
 * <p>This class does not hold any state, and if it weren't for {@linkplain #columnFromCondition(Object)}</p>, all
 * of its methods could be static. It's safe to create a singleton instance for use across threads.</p>
 * @param <C> the condition type.
 */
public abstract class MultiIndexEnumerator<C> {
    
    Logger log = LoggerFactory.getLogger(MultiIndexEnumerator.class);
    
    protected abstract Column columnFromCondition(C condition);
    
    public Collection<MultiIndexPair<C>> get(Collection<? extends Index> indexes, Set<C> conditions,
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
        
        Set<MultiIndexPair<C>> results = new HashSet<MultiIndexPair<C>>();
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
                    emit(outerCandidate, innerCandidate, results, columnEquivalences);
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

    private void emit(MultiIndexCandidate<C> first, MultiIndexCandidate<C> second,
                      Collection<MultiIndexPair<C>> output, EquivalenceFinder<Column> columnEquivalences)
    {
        if (!first.equals(second)) {
            Table firstTable = first.getIndex().leafMostTable();
            Table secondTable = second.getIndex().leafMostTable();
            if (firstTable.isUserTable() && secondTable.isUserTable()) {
                List<Column> commonTrailing = getCommonTrailing(first, second, columnEquivalences);
                UserTable firstUTable = (UserTable) firstTable;
                UserTable secondUTable = (UserTable) secondTable;
                // handle the two single-branch cases
                if (firstUTable.isDescendantOf(secondUTable)
                        && includesHKey(secondUTable, commonTrailing, columnEquivalences)) {
                    output.add(new MultiIndexPair<C>(first, second));
                }
                else if (secondUTable.isDescendantOf(firstUTable)
                        && includesHKey(firstUTable, commonTrailing, columnEquivalences)) {
                    output.add(new MultiIndexPair<C>(second, first));
                }
                else {
                    // TODO -- enable when multi-branch is in
                    // output.add(new MultiIndexPair(first, second));
                    // output.add(new MultiIndexPair(second, first));
                }
            }
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

    private List<Column> getCommonTrailing(MultiIndexCandidate<C> first, MultiIndexCandidate<C> second,
                                           EquivalenceFinder<Column> columnEquivalences)
    {
        List<Column> firstTrailing = first.getUnpeggedColumns();
        if (firstTrailing.isEmpty())
            return Collections.emptyList();
        List<Column> secondTrailing = second.getUnpeggedColumns();
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

    public static class MultiIndexPair<C> {
        MultiIndexCandidate<C> outputIndex;
        MultiIndexCandidate<C> selectorIndex;

        public MultiIndexPair(MultiIndexCandidate<C> outputIndex, MultiIndexCandidate<C> selectorIndex) {
            this.outputIndex = outputIndex;
            this.selectorIndex = selectorIndex;
        }

        public MultiIndexCandidate<C> getOutputIndex() {
            return outputIndex;
        }

        public MultiIndexCandidate<C> getSelectorIndex() {
            return selectorIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            MultiIndexPair that = (MultiIndexPair) o;

            return outputIndex.equals(that.outputIndex) && selectorIndex.equals(that.selectorIndex);

        }

        @Override
        public int hashCode() {
            int result = outputIndex.hashCode();
            result = 31 * result + selectorIndex.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return String.format("(%s, %s)", outputIndex, selectorIndex);
        }
    }
}
