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

import com.akiban.server.error.NoSuchFunctionException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.texpressions.TValidatedAggregator;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.akiban.server.types3.texpressions.TValidatedResolvable;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class OverloadResolver {

    public class OverloadResult<V extends TValidatedResolvable> {
        private V overload;
        private Map<TInputSet, TInstance> instances;

        private OverloadResult(V overload, List<? extends TPreptimeValue> inputs)
        {
            this.overload = overload;
            List<TInputSet> inputSets = overload.inputSets();
            this.instances = new HashMap<TInputSet, TInstance>(inputSets.size());
            for (TInputSet inputSet : inputSets) {
                final TInstance instance;
                TClass targetTClass = inputSet.targetType();
                if (targetTClass == null)
                    instance = findCommon(overload, inputSet, inputs);
                else
                    instance = findInstance(overload, inputSet, inputs);
                if (instance != null) {
                    boolean nullable = nullable(overload,  inputSet, inputs);
                    instance.setNullable(nullable);
                }
                instances.put(inputSet, instance);
            }
        }

        private boolean nullable(V overload, TInputSet inputSet,
                                 List<? extends TPreptimeValue> inputs)
        {
            for (int i = 0, size = inputs.size(); i < size; ++i) {
                if (overload.inputSetAt(i) != inputSet)
                    continue;
                TPreptimeValue input = inputs.get(i);
                if (input == null || input.isNullable())
                    return true;
            }
            return false;
        }

        private TInstance findInstance(V overload, TInputSet inputSet,
                                       List<? extends TPreptimeValue> inputs)
        {
            final TClass targetTClass = inputSet.targetType();
            assert targetTClass != null;

            TInstance result = null;
            int lastPositionalInput = overload.positionalInputs();
            boolean notVararg = ! overload.isVararg();
            for (int i = 0, size = inputs.size(); i < size; ++i) {
                if (overload.inputSetAt(i) != inputSet)
                    continue;
                if (notVararg && (i >= lastPositionalInput))
                    break;
                TInstance inputInstance = inputs.get(i).instance();
                TClass inputClass = (inputInstance == null) ? null : inputInstance.typeClass();
                if (inputClass == targetTClass) {
                    result = (result == null)
                            ? inputInstance
                            : targetTClass.pickInstance(result, inputInstance);
                }
            }
            if (result == null)
                result = targetTClass.instance();
            return result;
        }

        private TInstance findCommon(V overload, TInputSet inputSet,
                                  List<? extends TPreptimeValue> inputs)
        {
            assert inputSet.targetType() == null : inputSet; // so we have to look at inputs
            TClass common = null;
            TInstance commonInst = null;
            int lastPositionalInput = overload.positionalInputs();
            boolean notVararg = ! overload.isVararg();
            for (int i = 0, size = inputs.size(); i < size; ++i) {
                if (overload.inputSetAt(i) != inputSet)
                    continue;
                if (notVararg && (i >= lastPositionalInput))
                    break;
                TInstance inputInstance = inputs.get(i).instance();
                if (inputInstance == null) {
                    // unknown type, like a NULL literal or parameter
                    continue;
                }
                TClass inputClass = inputInstance.typeClass();
                if (common == null) {
                    // First input we've seen, so just use it.
                    common = inputClass;
                    commonInst = inputInstance;
                }
                else if (inputClass == common) {
                    // saw the same TClass as before, so pick it
                    commonInst = (commonInst == null) ? inputInstance : common.pickInstance(commonInst, inputInstance);
                }
                else {
                    // Saw a different TCLass as before, so need to cast one of them. We'll get the new common type,
                    // at which point we have exactly one of three possibilities:
                    //   1) newCommon == [old] common, in which case we'll keep the old TInstance
                    //   2) newCommon == inputClass, in which case we'll use the inputInstance
                    //   3) newCommon is neither, in which case we'll generate a new TInstance
                    // We know that we can't have both #1 and #2, because that would imply [old] common == inputClass,
                    // which has already been handled.
                    TClass newCommon = commonTClass(common, inputClass);
                    if (newCommon == null)
                        throw new OverloadException(overload + ": couldn't find common types for " + inputSet
                            + " with " + inputs);

                    if (newCommon == inputClass) { // case #2
                        common = newCommon;
                        commonInst = inputInstance;
                    }
                    else if (newCommon != common) { // case #3
                        common = newCommon;
                        commonInst = null;
                    }
                    // else if (newCommon = common), we don't need this because there's nothing to do in this case
                }
            }
            if (common == null)
                throw new OverloadException("couldn't resolve type for " + inputSet + " with " + inputs);
            return (commonInst == null)
                ? common.instance()
                : commonInst;
        }

        public V getOverload() {
            return overload;
        }

        public TInstance getPickedInstance() {
            return instances.get(overload.pickingInputSet());
        }

        public TInstance getTypeClass(int inputIndex) {
            TInputSet tInputSet = overload.inputSetAt(inputIndex);
            return instances.get(tInputSet);
        }
    }

    private final T3RegistryService registry;

    public OverloadResolver(T3RegistryService registry) {
        this.registry = registry;
    }

    public TCast getTCast(TInstance source, TInstance target) {
        return registry.cast(source.typeClass(), target.typeClass());
    }

    public T3RegistryService getRegistry() {
        return registry;
    }

    /**
     * Returns the common of the two types. For either argument, a <tt>null</tt> value is interpreted as any type. At
     * least one of the input TClasses must be non-<tt>null</tt>. If one of the inputs is null, the result is always
     * the other input.
     * @param tClass1 the first type class
     * @param tClass2 the other type class
     * @return the common class, or <tt>null</tt> if none were found
     * @throws IllegalArgumentException if both inputs are <tt>null</tt>
     */
    public TClass commonTClass(TClass tClass1, TClass tClass2) {
        // NOTE:
        // This method shares some concepts with #reduceToMinimalCastGroups, but the two methods seem different enough
        // that they're best implemented without much common code. But this could be an opportunity for refactoring.

        // handle easy cases where one or the other is null
        if (tClass1 == null) {
            if (tClass2 == null)
                throw new IllegalArgumentException("both inputs can't be null");
            return tClass2;
        }
        if (tClass2 == null)
            return tClass1;

        // If they're the same class, this is a really easy question to answer.
        if (tClass1.equals(tClass2))
            return tClass1;

        // Alright, neither is null and they're both different. Try the hard way.
        Set<? extends TClass> t1Targets = registry.stronglyCastableFrom(tClass1);
        Set<? extends TClass> t2Targets = registry.stronglyCastableFrom(tClass2);

        // TODO: The following is not very efficient -- opportunity for optimization?

        // Sets.intersection works best when the first arg is smaller, so do that.
        Set<? extends TClass> set1, set2;
        if (t1Targets.size() < t2Targets.size()) {
            set1 = t1Targets;
            set2 = t2Targets;
        }
        else {
            set1 = t2Targets;
            set2 = t1Targets;
        }
        Set<? extends TClass> castGroup = Sets.intersection(set1, set2); // N^2 operation number 1

        // The cast group is the set of type classes such that for each element C of castGroup, both tClass1 and tClass2
        // can be strongly cast to C. castGroup is thus the set of common types for { tClass1, tClass2 }. We now need
        // to find the MOST SPECIFIC cast M such that any element of castGroup which is not M can be strongly castable
        // from M.
        if (castGroup.isEmpty())
            throw new OverloadException("no common types found for " + tClass1 + " and " + tClass2);

        // N^2 operation number 2...
        TClass mostSpecific = null;
        for (TClass candidate : castGroup) {
            if (isMostSpecific(candidate, castGroup)) {
                if (mostSpecific == null)
                    mostSpecific = candidate;
                else
                    return null;
            }
        }
        return mostSpecific;
    }

    public <V extends TValidatedResolvable> OverloadResult<V> get(String name, List<? extends TPreptimeValue> inputs,
                                                                     Class<V> overloadType) {
        Iterable<? extends ScalarsGroup<V>> scalarsGroup;
        // TODO CLEANUP: or we could just pass in the right overload registry...
        if (overloadType == TValidatedOverload.class)
            scalarsGroup = (Iterable<? extends ScalarsGroup<V>>) registry.getOverloads(name);
        else if (overloadType == TValidatedAggregator.class)
            scalarsGroup = (Iterable<? extends ScalarsGroup<V>>) registry.getAggregates(name);
        else
            throw new AssertionError("unrecognized overload type: " + overloadType);
        if (scalarsGroup == null) {
            throw new NoSuchFunctionException(name);
        }
        return inputBasedResolution(name, inputs, scalarsGroup);
    }

    private <V extends TValidatedResolvable> OverloadResult<V> inputBasedResolution(
            String name, List<? extends TPreptimeValue> inputs,
            Iterable<? extends ScalarsGroup<V>> scalarGroupsByPriority)
    {

        V mostSpecific = null;
        boolean sawRightArity = false;
        for (ScalarsGroup<V> scalarsGroup : scalarGroupsByPriority) {
            Collection<? extends V> namedOverloads = scalarsGroup.getOverloads();
            List<V> candidates = new ArrayList<V>(namedOverloads.size());
            for (V overload : namedOverloads) {
                if (!overload.coversNInputs(inputs.size()))
                    continue;
                sawRightArity = true;
                if (isCandidate(overload, inputs, scalarsGroup)) {
                    candidates.add(overload);
                }
            }
            if (candidates.isEmpty())
                continue; // try next priority group of namedOverloads
            if (candidates.size() == 1) {
                mostSpecific = candidates.get(0);
                break; // found one!
            } else {
                List<List<V>> groups = reduceToMinimalCastGroups(candidates);
                if (groups.size() == 1 && groups.get(0).size() == 1) {
                    mostSpecific = groups.get(0).get(0);
                    break; // found one!
                }
                else {
                    // this priority group had too many candidates; this is an error
                    throw overloadException(name, inputs);
                }
            }
        }
        if (mostSpecific == null) {
            // no priority group had any candidates; this is an error
            if (sawRightArity)
                throw overloadException(name, inputs);
            throw new WrongExpressionArityException(-1, inputs.size()); // TODO on expected inputs!
        }
        return buildResult(mostSpecific, inputs);
    }

    private OverloadException overloadException(String name, List<? extends TPreptimeValue> inputs) {
        StringBuilder sb = new StringBuilder("no suitable overload found for ");
        sb.append(name).append('(');
        for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
            TPreptimeValue tpv = inputs.get(i);
            if (tpv == null) {
                sb.append('?');
            }
            else {
                TInstance tInstance = tpv.instance();
                String className = (tInstance == null)
                        ? "?"
                        : tInstance.typeClass().name().toString();
                sb.append(className);
            }
            if ( (i+1) < inputsSize)
                sb.append(", ");
        }
        sb.append(')');
        return new OverloadException(sb.toString());
    }

    private boolean isMostSpecific(TClass candidate, Set<? extends TClass> castGroup) {
        for (TClass inner : castGroup) {
            if (candidate.equals(inner))
                continue;
            if (!stronglyCastable(candidate, inner)) {
                return false;
            }
        }
        return true;
    }

    private boolean stronglyCastable(TClass source, TClass target) {
        return isStrong(registry.cast(source, target));
    }

    private <V extends TValidatedResolvable> boolean isCandidate(V overload,
                                List<? extends TPreptimeValue> inputs,
                                ScalarsGroup<V> scalarGroups) {
        if (!overload.coversNInputs(inputs.size()))
            return false;

        for (int i = 0, inputsSize = inputs.size(); i < inputsSize; i++) {
            // alow this input if
            // all overloads of this name have the same at this position
            if (scalarGroups.hasSameTypeAt(i))
                continue;

            TPreptimeValue inputTpv = inputs.get(i);
            TInstance inputInstance = (inputTpv == null) ? null : inputTpv.instance();
            // allow this input if...
            // ... input's type it NULL or ?
            if (inputInstance == null)       // input
                continue;
            // ... input set takes ANY
            TInputSet inputSet = overload.inputSetAt(i);
            if (inputSet.targetType() == null)
                continue;
            // ... input can be strongly cast to input set
            if (inputSet.isExact()) {
                if (inputInstance.typeClass() == inputSet.targetType())
                    continue;
            }
            else {
                TCast cast = registry.cast(inputInstance.typeClass(), inputSet.targetType());
                if (isStrong(cast))
                    continue;
            }
            // This input precludes the use of the overload
            return false;
        }
        // no inputs have precluded this overload
        return true;
    }

    private <V extends TValidatedResolvable> OverloadResult<V> buildResult(
            V overload, List<? extends TPreptimeValue> inputs)
    {
        return new OverloadResult<V>(overload, inputs);
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
    private <V extends TValidatedResolvable> List<List<V>> reduceToMinimalCastGroups(List<V> candidates) {
        // NOTE:
        // This method shares some concepts with #commonTClass. See that method for a note about possible refactoring
        // opportunities (tl;dr is the two methods don't share code right now, but they might be able to.)
        List<List<V>> castGroups = new ArrayList<List<V>>();

        for(V B : candidates) {
            final int nInputSets = B.inputSets().size();

            // Find the OVERLOAD CAST GROUP
            List<V> castGroup = null;
            for(List<V> group : castGroups) {
                // Groups are not empty, can always get first
                V cur = group.get(0);
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
                Iterator<V> it = castGroup.iterator();
                while(it.hasNext()) {
                    V A = it.next();
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
                castGroup = new ArrayList<V>(1);
                castGroup.add(B);
                castGroups.add(castGroup);
            }
        }
        return castGroups;
    }

    private boolean isStrong(TCast cast) {
        return (cast != null) && registry.isStrong(cast);
    }

    // TODO replace with InvalidOperationExceptions
    static class OverloadException extends RuntimeException {
        private OverloadException(String message) {
            super(message);
        }
    }
}
