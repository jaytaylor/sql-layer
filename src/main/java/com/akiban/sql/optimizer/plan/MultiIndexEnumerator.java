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

import com.akiban.ais.model.Index;
import com.akiban.ais.model.Table;
import com.akiban.ais.model.UserTable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * <p>An enumerator which, given an Index and a set of conditions, will produce a collection of
 * {@link MultiIndexCandidateBase} which represent valid multi-index intersection pairs.</p>
 * 
 * <p>Like {@link MultiIndexCandidateBase}, this class is generic and abstract; its only abstract method is one for
 * creating an empty {@link MultiIndexCandidateBase}. Also like that class, the expectation is that there will be two
 * subclasses for this class: one for unit testing, and one for production.</p>
 * 
 * <p>This class does not hold any state, and if it weren't for {@linkplain #createSeedCandidate(Index, Set)}</p>, all
 * of its methods could be static. It's safe to create a singleton instance for use across threads.</p>
 * @param <C> the condition type.
 */
public abstract class MultiIndexEnumerator<C> {

    protected abstract MultiIndexCandidateBase<C> createSeedCandidate(Index index, Set<C> conditions);
    
    public Collection<MultiIndexPair<C>> get(Collection<? extends Index> indexes, Set<C> conditions) {
        // note: "inner" and "outer" here refer only to these loops, nothing more.
        Set<MultiIndexPair<C>> results = new HashSet<MultiIndexPair<C>>();
        for (Index outerIndex : indexes) {
            List<MultiIndexCandidateBase<C>> outerCandidates = new ArrayList<MultiIndexCandidateBase<C>>();
            buildCandidate(createSeedCandidate(outerIndex, conditions), outerCandidates);
            for (MultiIndexCandidateBase<C> outerCandidate : outerCandidates) {
                if (outerCandidate.anyPegged()) {
                    C outerLastPegged = outerCandidate.getLastPegged();
                    for (Index innerIndex : indexes) {
                        List<MultiIndexCandidateBase<C>> innerCandidates = new ArrayList<MultiIndexCandidateBase<C>>();
                        buildCandidate(createSeedCandidate(innerIndex, conditions), innerCandidates);
                        for (MultiIndexCandidateBase<C> innerCandidate : innerCandidates) {
                            if (!outerLastPegged.equals(innerCandidate.getLastPegged())) {
                                emit(outerCandidate, innerCandidate, results);
                            }
                        }
                    }
                }
            }
        }
        return results;
    }

    private void emit(MultiIndexCandidateBase<C> first, MultiIndexCandidateBase<C> second,
                      Collection<MultiIndexPair<C>> output)
    {
        if (!first.equals(second)) {
            Table firstTable = first.getIndex().leafMostTable();
            Table secondTable = second.getIndex().leafMostTable();
            if (firstTable.isUserTable() && secondTable.isUserTable()) {
                UserTable firstUTable = (UserTable) firstTable;
                UserTable secondUTable = (UserTable) secondTable;
                // handle the two single-branch cases
                if (firstUTable.isDescendantOf(secondUTable)) {
                    output.add(new MultiIndexPair<C>(first, second));
                }
                else if (secondUTable.isDescendantOf(firstUTable)) {
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

    private void buildCandidate(MultiIndexCandidateBase<C> candidate, Collection<MultiIndexCandidateBase<C>> output)
    {
        Collection<C> peggable = candidate.findPeggable();
        boolean nonePegged = true;
        for (Iterator<C> iterator = peggable.iterator(); iterator.hasNext(); ) {
            C condition = iterator.next();
            MultiIndexCandidateBase<C> newCandidate;
            if (iterator.hasNext()) {
                newCandidate = createSeedCandidate(candidate.getIndex(), candidate.getUnpeggedCopy());
                newCandidate.pegAll(candidate.getPegged());
            }
            else {
                newCandidate = candidate; // if this is the last condition, just reuse this instance
            }
            candidate.peg(condition);
            buildCandidate(newCandidate, output);
            nonePegged = false;
        }
        if (nonePegged && candidate.anyPegged()) {
            output.add(candidate);
        }
    }

    public static class MultiIndexPair<C> {
        MultiIndexCandidateBase<C> outputIndex;
        MultiIndexCandidateBase<C> selectorIndex;

        public MultiIndexPair(MultiIndexCandidateBase<C> outputIndex, MultiIndexCandidateBase<C> selectorIndex) {
            this.outputIndex = outputIndex;
            this.selectorIndex = selectorIndex;
        }

        public MultiIndexCandidateBase<C> getOutputIndex() {
            return outputIndex;
        }

        public MultiIndexCandidateBase<C> getSelectorIndex() {
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
