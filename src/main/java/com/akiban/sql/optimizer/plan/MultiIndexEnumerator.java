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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class MultiIndexEnumerator {

    public Collection<MultiIndexPair> get(Set<? extends Index> indexes, Set<ComparisonCondition> conditions) {
        // note: "inner" and "outer" here refer only to these loops, nothing more.
        List<MultiIndexPair> results = new ArrayList<MultiIndexPair>();
        for (Index outerIndex : indexes) {
            List<MultiIndexCandidate> outerCandidates = new ArrayList<MultiIndexCandidate>();
            buildCandidate(new MultiIndexCandidate(outerIndex, conditions), outerCandidates);
            for (MultiIndexCandidate outerCandidate : outerCandidates) {
                for (Index innerIndex : indexes) {
                    Set<ComparisonCondition> innerConditions
                            = (innerIndex == outerIndex)
                            ? outerCandidate.getUnpeggedConditions() // same idx, but only on the remaining conditions
                            : conditions;
                    List<MultiIndexCandidate> innerCandidates = new ArrayList<MultiIndexCandidate>();
                    buildCandidate(new MultiIndexCandidate(innerIndex, innerConditions), innerCandidates);
                    for (MultiIndexCandidate innerCandidate : innerCandidates) {
                        emit(outerCandidate, innerCandidate, results);
                    }
                }
            }
        }
        return results;
    }

    private void emit(MultiIndexCandidate first, MultiIndexCandidate second, List<MultiIndexPair> output) {
        Table firstTable = first.getIndex().leafMostTable();
        Table secondTable = second.getIndex().leafMostTable();
        if (firstTable.isUserTable() && secondTable.isUserTable()) {
            UserTable firstUTable = (UserTable) firstTable;
            UserTable secondUTable = (UserTable) secondTable;
            // handle the two single-branch cases
            if (firstUTable.isDescendantOf(secondUTable)) {
                output.add(new MultiIndexPair(first, second));
            }
            else if (secondUTable.isDescendantOf(firstUTable)) {
                output.add(new MultiIndexPair(second, first));
            }
            else {
                // TODO -- enable when multi-branch is in
                // output.add(new MultiIndexPair(first, second));
                // output.add(new MultiIndexPair(second, first));
            }
        }
    }

    private void buildCandidate(MultiIndexCandidate candidate, Collection<MultiIndexCandidate> output)
    {
        Collection<ComparisonCondition> peggable = candidate.findPeggable();
        boolean nonePegged = true;
        for (Iterator<ComparisonCondition> iterator = peggable.iterator(); iterator.hasNext(); ) {
            ComparisonCondition condition = iterator.next();
            MultiIndexCandidate newCandidate = iterator.hasNext()
                    ? new MultiIndexCandidate(candidate)
                    : candidate; // if this is the last condition, just reuse this instance
            candidate.peg(condition);
            buildCandidate(newCandidate, output);
            nonePegged = false;
        }
        if (nonePegged && candidate.anyPegged()) {
            output.add(candidate);
        }
    }

    public static class MultiIndexPair {
        MultiIndexCandidate outputIndex;
        MultiIndexCandidate selectorIndex;

        public MultiIndexPair(MultiIndexCandidate outputIndex, MultiIndexCandidate selectorIndex) {
            this.outputIndex = outputIndex;
            this.selectorIndex = selectorIndex;
        }

        public MultiIndexCandidate getOutputIndex() {
            return outputIndex;
        }

        public MultiIndexCandidate getSelectorIndex() {
            return selectorIndex;
        }
    }
}
