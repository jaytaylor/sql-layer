/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInputSet;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TOverload;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.util.SparseArray;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TValidatedOverload implements TOverload {
    // TResolvable methods (straight delegation)

    @Override
    public String displayName() {
        return overload.displayName();
    }

    @Override
    public String[] registeredNames()
    {
        return overload.registeredNames();
    }

    @Override
    public String id() {
        return overload.id();
    }

    @Override
    public int[] getPriorities() {
        return overload.getPriorities();
    }

    @Override
    public boolean equals(Object obj) {
        return (obj instanceof TOverload) && overload.equals(obj);
    }

    @Override
    public int hashCode() {
        return overload.hashCode();
    }

    // TResolvable methods (cached)

    @Override
    public List<TInputSet> inputSets() {
        return inputSetsCached;
    }

    @Override
    public TOverloadResult resultType() {
        return resultStrategy;
    }

    @Override
    public InputSetFlags exactInputs() {
        return exactInputs;
    }

    // TValidatedResolvable methods

    public TOverload getUnderlying() {
        return overload;
    }

    // TValidatedOverload methods

    public int firstVarargInput() {
        if (varargs == null)
            return -1;
        return inputSetsByPos.size();
    }

    public TInputSet pickingInputSet() {
        return pickingSet;
    }

    public TInputSet varargInputSet() {
        return varargs;
    }

    public boolean isVararg() {
        return varargs != null;
    }

    public boolean coversNInputs(int nInputs) {
        /* no pos           : nInputs = 0
         * POS(N)           : nInputs = N+1
         * REMAINING        : nInputs >= 0
         * POS(N),REMAINING : nInputs >= N+1
         */
        int minSize = inputSetsByPos.size();
        return (varargs == null) ? (nInputs == minSize) : (nInputs >= minSize);
    }

    public int positionalInputs() {
        return inputSetsByPos.size();
    }

    public int inputSetIndexAtPos(int atPosition) {
        if (atPosition < 0)
            throw new IllegalArgumentException("atPosition must be non-negative: " + atPosition);
        if (atPosition >= inputsToInputSetIndex.length) {
            if (!isVararg())
                throw new IllegalArgumentException("out of range for non-vararg: " + atPosition);
            atPosition = inputsToInputSetIndex.length - 1;
        }
        return inputsToInputSetIndex[atPosition];
    }

    public int inputSetIndexCount() {
        return inputsToInputSetIndex.length;
    }

    public TInputSet inputSetAt(int index) {
        if(index >= inputSetsByPos.size()) {
            if(varargs == null) {
                throw new IllegalArgumentException("No such input set: " + index);
            }
            return varargs;
        }
        return inputSetsByPos.get(index);
    }

    public TOverloadResult resultStrategy() {
        return resultStrategy;
    }

    public int firstInput(TInputSet inputSet) {
        int result = inputSet.firstPosition();
        if (result < 0 && inputSet.isPicking())
            result = firstVarargInput();
        assert result >= 0 : result + " in " + inputSet + " within " + this;
        return result;
    }

    public int nextInput(TInputSet inputSet, int i, int max) {
        if (i >= max)
            return -1;
        int result = inputSet.nextPosition(i);
        if (result < 0 && inputSet.coversRemaining())
            result = Math.max(i, inputSetsByPos.size());
        return result;
    }

    public String describeInputs() {
        StringBuilder sb = new StringBuilder();
        buildInputsDescription(sb);
        return sb.toString();
    }

    // Redefine toString

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(overload.displayName()).append('(');
        buildInputsDescription(sb);
        sb.append(") -> ").append(resultStrategy);
        return sb.toString();
    }

    // package-private

    boolean coversExactlyNArgs(int nargs) {
        return (!isVararg()) && inputSetsByPos.size() == nargs;
    }

    TValidatedOverload(TOverload overload) {
        this(overload, overload.inputSets());
    }

    TValidatedOverload(TOverload overload, List<TInputSet> inputSets) {
        TInputSet localVarargInputs = null;
        TInputSet localPickingInputs = null;
        SparseArray<TInputSet> inputSetsArray = new SparseArray<>();
        this.inputSetsCached = new ArrayList<>(inputSets);
        for (TInputSet inputSet : inputSetsCached) {
            if (inputSet.coversRemaining()) {
                if (localVarargInputs != null)
                    throw new InvalidOverloadException("multiple input sets are vararg");
                localVarargInputs = inputSet;
            }
            for (int i = 0, max = inputSet.positionsLength(); i < max; ++i) {
                if (inputSet.covers(i)) {
                    if (inputSetsArray.isDefined(i))
                        throw new InvalidOverloadException("multiple input sets cover input " + i);
                    inputSetsArray.set(i, inputSet);
                }
            }
            if (inputSet.isPicking()) {
                if (localPickingInputs != null)
                    throw new InvalidOverloadException("overloads can't define multiple picking input sets");
                localPickingInputs = inputSet;
            }
        }
        if (!inputSetsArray.isCompactable())
            throw new InvalidOverloadException("not all inputs covered");
        this.overload = overload;
        this.inputSetsByPos = inputSetsArray.toList();
        this.varargs = localVarargInputs;
        this.resultStrategy = overload.resultType();
        this.pickingSet = localPickingInputs;
        this.inputSetDescriptions = createInputSetDescriptions(inputSetsByPos, pickingSet, varargs);
        this.exactInputs = overload.exactInputs();
        this.inputsToInputSetIndex = mapInputsToInputSetIndex(inputSetsByPos, inputSetsCached, varargs);
    }

    private void buildInputsDescription(StringBuilder sb) {
        for (int i = 0, nPos = positionalInputs(), nDesc = inputSetDescriptions.length; i < nDesc; ++i) {
            sb.append(inputSetDescriptions[i]);
            if (i == nPos)
                sb.append("...");
            if (i+1 < nDesc)
                sb.append(", ");
        }
    }

    private static int[] mapInputsToInputSetIndex(List<TInputSet> inputSetsByPos,
                                                  List<TInputSet> inputSetsCached,
                                                  TInputSet varargs)
    {
        int naturalPositions = inputSetsByPos.size();
        int positions = naturalPositions;
        if (varargs != null && varargs.positionsLength() == 0)
            ++positions;
        int[] results = new int[positions];
        Map<TInputSet, Integer> inputSetsToIndex = new HashMap<>(positions);
        int indexCounter = 0;
        for (int i = 0; i < positions; ++i) {
            TInputSet inputSet = (i < naturalPositions) ? inputSetsByPos.get(i) : varargs;
            Integer inputSetIndex = inputSetsToIndex.get(inputSet);
            if (inputSetIndex == null) {
                inputSetIndex = indexCounter++;
                inputSetsToIndex.put(inputSet, inputSetIndex);
            }
            results[i] = inputSetIndex;
        }
        return results;
    }

    private static String[] createInputSetDescriptions(List<TInputSet> inputSetsByPos,
                                                       TInputSet pickingSet, TInputSet varargInputSet)
    {
        int nInputsRaw = inputSetsByPos.size();
        int nInputsExtended = (varargInputSet == null) ? nInputsRaw : (nInputsRaw + 1);
        String[] result = new String[nInputsExtended];
        Map<TInputSet,String> map = new HashMap<>(nInputsRaw);
        int anyCount = 0;
        // if the picking input set is T, it's always T (not T#1 etc)
        if (pickingSet != null && pickingSet.targetType() == null) {
            map.put(pickingSet, "T");
            ++anyCount;
        }
        for (int i = 0; i < nInputsExtended; i++) {
            TInputSet inputSet = (i == nInputsRaw) ? varargInputSet : inputSetsByPos.get(i);
            String description = map.get(inputSet);
            if (description == null) {
                TClass inputTClass = inputSet == null ? null : inputSet.targetType();
                if (inputTClass == null) {
                    description = "T";
                    if (anyCount > 0)
                        description += ('#' + anyCount);
                    ++anyCount;
                } else {
                    description = inputTClass.name().unqualifiedName();
                }
                map.put(inputSet, description);
            }
            result[i] = description;
        }
        return result;
    }
    private final TOverload overload;
    private final List<TInputSet> inputSetsCached;
    private final List<TInputSet> inputSetsByPos;
    private final TOverloadResult resultStrategy;
    private final TInputSet varargs;

    private final TInputSet pickingSet;
    private final InputSetFlags exactInputs;
    private final int[] inputsToInputSetIndex;

    /**
     * A description of each input, indexed by its position. If there is a vararg input, its index is
     * one greater than the 0-indexing of positions.
     */
    private final String[] inputSetDescriptions;

    private static class InvalidOverloadException extends RuntimeException {
        private InvalidOverloadException(String message) {
            super(message);
        }
    }
}
