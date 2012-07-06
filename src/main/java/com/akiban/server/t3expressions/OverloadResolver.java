/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */
package com.akiban.server.t3expressions;

import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types3.TAggregator;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.util.Equality;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class OverloadResolver {

    public static class OverloadResult {
        private TValidatedOverload overload;

        private TInstance pickedInstance;
        public OverloadResult(TValidatedOverload overload, TInstance pickedInstance) {
            this.overload = overload;
            this.pickedInstance = pickedInstance;
        }

        public TValidatedOverload getOverload() {
            return overload;
        }

        public TInstance getPickedInstance() {
            return pickedInstance;
        }

        public TClass getTypeInstance(int inputIndex) {
            throw new UnsupportedOperationException(); // TODO
        }
    }

    private final T3ScalarsRegistry registry;
    private final T3AggregatesRegistry aggregatesRegistry;

    /**
     * For testing. Aggregates will not work.
     * @param registry the scalar registry
     */
    OverloadResolver(T3ScalarsRegistry registry) {
        this(registry, null);
    }

    public OverloadResolver(T3ScalarsRegistry registry, T3AggregatesRegistry aggregatesRegistry) {
        this.registry = registry;
        this.aggregatesRegistry = aggregatesRegistry;
    }

    public TClassPossibility commonTClass(TClass tClass1, TClass tClass2) {
        return registry.commonTClass(tClass1, tClass2);
    }

    public OverloadResult get(String name, List<? extends TPreptimeValue> inputs) {
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

    public TAggregator getAggregation(String name, TClass inputType) {
        List<TAggregator> candidates = new ArrayList<TAggregator>(aggregatesRegistry.getAggregates(name));
        for (TAggregator candidate : candidates) {
            if (Equality.areEqual(candidate.getTypeClass(), inputType))
                return candidate;
            // TODO use casting types, etc
        }
        throw new AkibanInternalException("no appropriate aggregate found for " + name + "(" + inputType + ")");
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
        TInstance pickingInstance = pickingInstance(overload, inputs);
        return new OverloadResult(overload, pickingInstance);
    }

    private TInstance pickingInstance(TValidatedOverload overload, List<? extends TPreptimeValue> inputs) {
        TInputSet pickingSet = overload.pickingInputSet();
        if (pickingSet == null) {
            return null;
        }
        TClass common = null; // TODO change to TInstance, so we can more precisely pick instances
        for (int i = pickingSet.firstPosition(); i >=0 ; i = pickingSet.nextPosition(i)) {
            TInstance instance = inputs.get(i).instance();
            common = registry.commonTClass(common, instance != null ? instance.typeClass() : null).get();
            if (common == T3ScalarsRegistry.NO_COMMON)
                return common.instance(); // TODO shouldn't we throw an exception?
        }
        if (pickingSet.coversRemaining()) {
            for (int i = overload.firstVarargInput(), last = inputs.size(); i < last; ++i) {
                TInstance instance = inputs.get(i).instance();
                common = registry.commonTClass(common, instance != null ? instance.typeClass() : null).get();
                if (common == T3ScalarsRegistry.NO_COMMON)
                    return common.instance(); // TODO shouldn't we throw an exception?
            }
        }
        assert common != null : "no common type found";
        return common.instance();
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
