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
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInputSet;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.texpressions.TValidatedOverload;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public final class OverloadResolver<V extends TValidatedOverload> {

    public static class OverloadResult<V extends TValidatedOverload> {
        private V overload;
        private Map<TInputSet, TInstance> instances;

        private OverloadResult(V overload, List<? extends TPreptimeValue> inputs, TCastResolver resolver)
        {
            this.overload = overload;
            List<TInputSet> inputSets = overload.inputSets();
            this.instances = new HashMap<TInputSet, TInstance>(inputSets.size());
            for (TInputSet inputSet : inputSets) {
                final TInstance instance;
                TClass targetTClass = inputSet.targetType();
                if (targetTClass == null)
                    instance = findCommon(overload, inputSet, inputs, resolver);
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
                                  List<? extends TPreptimeValue> inputs, TCastResolver resolver)
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
                    TClass newCommon = resolver.commonTClass(common, inputClass);
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

    private final ResolvablesRegistry<V> overloadsRegistry;
    private final TCastResolver castsResolver;

    public OverloadResolver(ResolvablesRegistry<V> overloadsRegistry, TCastResolver castsResolver) {
        this.overloadsRegistry = overloadsRegistry;
        this.castsResolver = castsResolver;
    }

    public OverloadResult<V> get(String name, List<? extends TPreptimeValue> inputs)
    {
        Iterable<? extends ScalarsGroup<V>> scalarsGroup = overloadsRegistry.get(name);
        if (scalarsGroup == null) {
            throw new NoSuchFunctionException(name);
        }
        return inputBasedResolution(name, inputs, scalarsGroup);
    }

    private OverloadResult<V> inputBasedResolution(
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

    ResolvablesRegistry<V> getRegistry() {
        return overloadsRegistry;
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

    private boolean isCandidate(V overload,
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
                if (castsResolver.strongCastExists(inputInstance.typeClass(), inputSet.targetType()))
                    continue;
            }
            // This input precludes the use of the overload
            return false;
        }
        // no inputs have precluded this overload
        return true;
    }

    private OverloadResult<V> buildResult(V overload, List<? extends TPreptimeValue> inputs)
    {
        return new OverloadResult<V>(overload, inputs, castsResolver);
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
    private List<List<V>> reduceToMinimalCastGroups(List<V> candidates) {
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
                        AtoB &= castsResolver.strongCastExists(Ai.targetType(), Bi.targetType());
                        BtoA &= castsResolver.strongCastExists(Bi.targetType(), Ai.targetType());
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


}
