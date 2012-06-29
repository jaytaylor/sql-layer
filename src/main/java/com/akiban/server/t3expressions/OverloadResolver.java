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
            List<List<TValidatedOverload>> groups = reduceToMinimalCastGroups(candidates);
            if (groups.size() == 1 && groups.get(0).size() == 1)
                mostSpecific = groups.get(0).get(0);
            // else: 0 or >1 candidates
            // TODO: Throw or let registry handle it?
        }
        if (mostSpecific == null)
            return null;
        return buildResult(mostSpecific, inputs);
    }

    private OverloadResult defaultResolution(List<? extends TPreptimeValue> inputs,
                                             Collection<? extends TValidatedOverload> namedOverloads) {
        int nInputs = inputs.size();
        TValidatedOverload resolvedOverload = namedOverloads.iterator().next();
        // throwing an exception here isn't strictly required, but it gives the user a more specific error
        if (!resolvedOverload.coversNInputs(nInputs))
            throw new WrongExpressionArityException(resolvedOverload.positionalInputs(), nInputs);
        return buildResult(resolvedOverload, inputs);
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

    private OverloadResult buildResult(TValidatedOverload overload, List<? extends TPreptimeValue> inputs) {
        TClass pickingClass = pickingClass(overload, inputs);
        return new OverloadResult(overload, pickingClass);
    }

    private TClass pickingClass(TValidatedOverload overload, List<? extends TPreptimeValue> inputs) {
        TInputSet pickingSet = overload.pickingInputSet();
        if (pickingSet == null) {
            return null;
        }
        TClass common = null;
        for (int i = pickingSet.firstPosition(); i >=0 ; i = pickingSet.nextPosition(i)) {
            TInstance instance = inputs.get(i).instance();
            common = registry.commonTClass(common, instance != null ? instance.typeClass() : null).get();
            if (common == T3ScalarsRegistry.NO_COMMON)
                return common;
        }
        if (pickingSet.coversRemaining()) {
            for (int i = overload.firstVarargInput(), last = inputs.size(); i < last; ++i) {
                TInstance instance = inputs.get(i).instance();
                common = registry.commonTClass(common, instance != null ? instance.typeClass() : null).get();
                if (common == T3ScalarsRegistry.NO_COMMON)
                    return common;
            }
        }
        return common;
    }

    /*
     * Two overloads have SIMILAR INPUT SETS if they
     *   1) have the same number of input sets
     *   2) each input set from one overload covers the same columns as an input set from the other function
     *
     * For any two overloads A and B, if A and B have SIMILAR INPUT SETS, and the target type of each input
     * set Ai can be strongly cast to the target type of Bi, then A is said to be MORE SPECIFIC than A, and B
     * is discarded as a possible overload.
     */
    private List<List<TValidatedOverload>> reduceToMinimalCastGroups(List<TValidatedOverload> candidates) {
        List<List<TValidatedOverload>> castGroups = new ArrayList<List<TValidatedOverload>>();

        for(TValidatedOverload B : candidates) {
            final int nInputSets = B.inputSets().size();

            // Find the OVERLOAD CAST GROUP
            List<TValidatedOverload> castGroup = null;
            for(List<TValidatedOverload> group : castGroups) {
                // Groups are not empty, can always get first
                TValidatedOverload cur = group.get(0);
                if(cur.inputSets().size() == nInputSets) {
                    boolean matches = true;
                    for(int i = 0; i < nInputSets && matches; ++i) {
                        matches = (cur.inputSetAt(i).positionsLength() == B.inputSetAt(i).positionsLength());
                    }
                    if(matches) {
                        castGroup = group;
                        break;
                    }
                }
            }

            if(castGroup != null) {
                // Found group, check for more specific
                Iterator<TValidatedOverload> it = castGroup.iterator();
                while(it.hasNext()) {
                    TValidatedOverload A = it.next();
                    boolean AtoB = true;
                    boolean BtoA = true;
                    for(int i = 0; i < nInputSets; ++i) {
                        TInputSet Ai = A.inputSetAt(i);
                        TInputSet Bi = B.inputSetAt(i);
                        AtoB &= isStrong(registry.cast(Ai.targetType(), Bi.targetType()));
                        BtoA &= isStrong(registry.cast(Bi.targetType(), Ai.targetType()));
                    }
                    if(AtoB) {
                        // current more specific
                        B = null;
                        break;
                    } else if(BtoA) {
                        // new more specific
                        it.remove();
                    }
                }
                if(B != null) {
                    // No more specific existed or B was most specific
                    castGroup.add(B);
                }
            } else {
                // No matching group, must be in a new group
                castGroup = new ArrayList<TValidatedOverload>(1);
                castGroup.add(B);
                castGroups.add(castGroup);
            }
        }
        return castGroups;
    }

    private static boolean isStrong(TCast cast) {
        return (cast != null) && cast.isAutomatic();
    }
}
