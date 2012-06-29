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

package com.akiban.server.t3expressions;

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class OverloadResolver {
    public static class OverloadResult {
        private TValidatedOverload overload;
        private TClass pickingClass;

        public OverloadResult(TValidatedOverload overload, TClass pickingClass) {
            this.overload = overload;
            this.pickingClass = pickingClass;
        }

        public TValidatedOverload getOverload() {
            return overload;
        }

        public TClass getPickingClass() {
            return pickingClass;
        }
    }

    private final T3ScalarsRegistry registry;

    public OverloadResolver(T3ScalarsRegistry registry) {
        this.registry = registry;
    }

    OverloadResult get(String name, List<? extends TPreptimeValue> inputs) {
        Collection<? extends TValidatedOverload> namedOverloads = registry.getOverloads(name);
        if (namedOverloads == null || namedOverloads.isEmpty()) {
            throw new NoSuchFunctionException(name);
        }
        if (namedOverloads.size() == 1) {
            return defaultResolution(inputs, namedOverloads);
        } else {
            return inputBasedResolution(inputs, namedOverloads);
        }
    }

    private OverloadResult inputBasedResolution(List<? extends TPreptimeValue> inputs,
                                                Collection<? extends TValidatedOverload> namedOverloads) {
        List<TValidatedOverload> candidates = new ArrayList<TValidatedOverload>(namedOverloads.size());
        for (TValidatedOverload overload : namedOverloads) {
            if (isCandidate(overload, inputs)) {
                candidates.add(overload);
            }
        }
        TValidatedOverload mostSpecific = null;
        if (candidates.size() == 0)
            return null;
        if (candidates.size() == 1) {
            mostSpecific = candidates.get(0);
        } else {
            List<List<TValidatedOverload>> groups = groupAndEliminate(candidates);
            if(groups.size() == 1) {
                if(groups.get(0).size() == 1)
                    mostSpecific = groups.get(0).get(0);
            }
        }
        if (mostSpecific == null)
            return null;
        TClass pickingClass = pickingClass(mostSpecific, inputs);
        return new OverloadResult(mostSpecific, pickingClass);
    }

    private OverloadResult defaultResolution(List<? extends TPreptimeValue> inputs,
                                             Collection<? extends TValidatedOverload> namedOverloads) {
        TValidatedOverload resolvedOverload;
        TClass pickingClass;
        int nInputs = inputs.size();
        resolvedOverload = namedOverloads.iterator().next();
        // throwing an exception here isn't strictly required, but it gives the user a more specific error
        if (!resolvedOverload.coversNInputs(nInputs))
            throw new WrongExpressionArityException(resolvedOverload.positionalInputs(), nInputs);
        pickingClass = pickingClass(resolvedOverload, inputs);
        return new OverloadResult(resolvedOverload, pickingClass);
    }

    private boolean isCandidate(TValidatedOverload overload, List<? extends TPreptimeValue> inputs) {
        if (!overload.coversNInputs(inputs.size()))
            return false;
        for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
            TInstance inputInstance = inputs.get(i).instance();
            // allow this input if...
            // ... input's type it NULL or ?
            if (inputInstance == null)       // input
                continue;
            // ... input set takes ANY
            TInputSet inputSet = overload.inputSetAt(i);
            if (inputSet.targetType() == null)
                continue;
            // ... input can be strongly cast to input set
            TCast cast = registry.cast(inputInstance.typeClass(), inputSet.targetType());
            if (cast != null && cast.isAutomatic())
                continue;
            // This input precludes the use of the overload
            return false;
        }
        // no inputs have precluded this overload
        return true;
    }

    private TClass pickingClass(TValidatedOverload overload, List<? extends TPreptimeValue> inputs) {
        TInputSet pickingSet = overload.pickingInputSet();
        if (pickingSet == null) {
            return null;
        }
        TClass common = null;
        for (int i = pickingSet.firstPosition(); i >=0 ; i = pickingSet.nextPosition(i)) {
            common = registry.commonTClass(common, inputs.get(i).instance().typeClass()).get();
            if (common == T3ScalarsRegistry.NO_COMMON)
                return common;
        }
        if (pickingSet.coversRemaining()) {
            for (int i = overload.firstVarargInput(), last = inputs.size(); i < last; ++i) {
                common = registry.commonTClass(common, inputs.get(i).instance().typeClass()).get();
                if (common == T3ScalarsRegistry.NO_COMMON)
                    return common;
            }
        }
        return common;
    }

    /*
     * Two overloads have SIMILAR INPUT SETS if they
     * 1) have the same number of input sets
     * 2) each input set from one overload covers the same columns as an input set from the other function
     *
     * For any two overloads A and B, if A and B have SIMILAR INPUT SETS, and the target type of each input
     * set Ai can be strongly cast to the target type of Bi, then A is said to be MORE SPECIFIC than A, and B
     * is discarded as a possible overload.
     */
    private List<List<TValidatedOverload>> groupAndEliminate(List<TValidatedOverload> candidates) {
        List<List<TValidatedOverload>> groups = new ArrayList<List<TValidatedOverload>>();
        for(TValidatedOverload overload : candidates) {
            final int nInputSets = overload.inputSets().size();
            for(List<TValidatedOverload> group : groups) {
                Iterator<TValidatedOverload> similarIt = group.iterator();
                OUTER:
                while(similarIt.hasNext()) {
                    TValidatedOverload cur = similarIt.next();
                    if(overload == null || (cur.inputSets().size() != nInputSets))
                        break;
                    boolean aToB = true;
                    boolean bToA = true;
                    for(int i = 0; i < nInputSets; ++i) {
                        TInputSet aSet = cur.inputSetAt(i);
                        TInputSet bSet = overload.inputSetAt(i);
                        if(aSet.positionsLength() != bSet.positionsLength())
                            break OUTER;
                        aToB &= isStrong(registry.cast(aSet.targetType(), bSet.targetType()));
                        bToA &= isStrong(registry.cast(bSet.targetType(), aSet.targetType()));
                    }
                    // This group is matching
                    // Is current more specific and new?
                    if(aToB) {
                        // current more specific
                        overload = null;
                    } else if(bToA) {
                        // new more specific
                        similarIt.remove();
                        if(!similarIt.hasNext()) {
                            group.add(overload);
                            overload = null;
                        }
                    }
                }
            }
            // Not eliminated and no matching group, new group
            if(overload != null) {
                List<TValidatedOverload> group = new ArrayList<TValidatedOverload>();
                group.add(overload);
                groups.add(group);
            }
        }
        return groups;
    }

    private static boolean isStrong(TCast cast) {
        return (cast != null) && cast.isAutomatic();
    }
}
