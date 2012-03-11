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

public abstract class MultiIndexEnumerator<S> {

    protected abstract MultiIndexCandidateBase<S> createSeedCandidate(Index index, Set<S> conditions);
    
    public Collection<MultiIndexPair<S>> get(Collection<? extends Index> indexes, Set<S> conditions) {
        // note: "inner" and "outer" here refer only to these loops, nothing more.
        Set<MultiIndexPair<S>> results = new HashSet<MultiIndexPair<S>>();
        for (Index outerIndex : indexes) {
            List<MultiIndexCandidateBase<S>> outerCandidates = new ArrayList<MultiIndexCandidateBase<S>>();
            buildCandidate(createSeedCandidate(outerIndex, conditions), outerCandidates);
            for (MultiIndexCandidateBase<S> outerCandidate : outerCandidates) {
                if (outerCandidate.anyPegged()) {
                    S outerLastPegged = outerCandidate.getLastPegged();
                    for (Index innerIndex : indexes) {
                        List<MultiIndexCandidateBase<S>> innerCandidates = new ArrayList<MultiIndexCandidateBase<S>>();
                        buildCandidate(createSeedCandidate(innerIndex, conditions), innerCandidates);
                        for (MultiIndexCandidateBase<S> innerCandidate : innerCandidates) {
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

    private void emit(MultiIndexCandidateBase<S> first, MultiIndexCandidateBase<S> second,
                      Collection<MultiIndexPair<S>> output)
    {
        if (!first.equals(second)) {
            Table firstTable = first.getIndex().leafMostTable();
            Table secondTable = second.getIndex().leafMostTable();
            if (firstTable.isUserTable() && secondTable.isUserTable()) {
                UserTable firstUTable = (UserTable) firstTable;
                UserTable secondUTable = (UserTable) secondTable;
                // handle the two single-branch cases
                if (firstUTable.isDescendantOf(secondUTable)) {
                    output.add(new MultiIndexPair<S>(first, second));
                }
                else if (secondUTable.isDescendantOf(firstUTable)) {
                    output.add(new MultiIndexPair<S>(second, first));
                }
                else {
                    // TODO -- enable when multi-branch is in
                    // output.add(new MultiIndexPair(first, second));
                    // output.add(new MultiIndexPair(second, first));
                }
            }
        }
    }

    private void buildCandidate(MultiIndexCandidateBase<S> candidate, Collection<MultiIndexCandidateBase<S>> output)
    {
        Collection<S> peggable = candidate.findPeggable();
        boolean nonePegged = true;
        for (Iterator<S> iterator = peggable.iterator(); iterator.hasNext(); ) {
            S condition = iterator.next();
            MultiIndexCandidateBase<S> newCandidate;
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

    public static class MultiIndexPair<S> {
        MultiIndexCandidateBase<S> outputIndex;
        MultiIndexCandidateBase<S> selectorIndex;

        public MultiIndexPair(MultiIndexCandidateBase<S> outputIndex, MultiIndexCandidateBase<S> selectorIndex) {
            this.outputIndex = outputIndex;
            this.selectorIndex = selectorIndex;
        }

        public MultiIndexCandidateBase<S> getOutputIndex() {
            return outputIndex;
        }

        public MultiIndexCandidateBase<S> getSelectorIndex() {
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
