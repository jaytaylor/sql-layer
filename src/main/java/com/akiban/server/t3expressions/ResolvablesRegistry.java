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
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCommutativeOverloads;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.aksql.AkBundle;
import com.akiban.server.types3.common.types.NoAttrTClass;
import com.akiban.server.types3.service.InstanceFinder;
import com.akiban.server.types3.texpressions.TValidatedOverload;
import com.google.common.base.Function;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

final class ResolvablesRegistry<V extends TValidatedOverload> {

    public Iterable<? extends ScalarsGroup<V>> get(String name) {
        List<ScalarsGroup<V>> result = overloadsByName.get(name.toLowerCase());
        return result.isEmpty() ? null : result;
    }

    public Collection<? extends Map.Entry<String, ScalarsGroup<V>>> entriesByName() {
        return Collections.unmodifiableCollection(overloadsByName.entries());
    }

    public static <R extends TOverload, V extends TValidatedOverload>
    ResolvablesRegistry<V> create(InstanceFinder finder,
                                  TCastResolver castResolver,
                                  Class<R> plainClass,
                                  Function<R, V> validator,
                                  Function<V, V> commutor)
    {
        ListMultimap<String, ScalarsGroup<V>> overloadsByName = createScalars(finder, castResolver, plainClass,
                                                                              validator, commutor);
        return new ResolvablesRegistry<V>(overloadsByName);
    }

    ResolvablesRegistry(ListMultimap<String, ScalarsGroup<V>> overloadsByName) {
        this.overloadsByName = overloadsByName;
    }

    private static <R extends TOverload, V extends TValidatedOverload>
    ListMultimap<String, ScalarsGroup<V>> createScalars(InstanceFinder finder,
                                                        TCastResolver castResolver,
                                                        Class<R> plainClass,
                                                        Function<R, V> validator,
                                                        Function<V, V> commutor)
    {

        Set<TOverload> commutedOverloads;
        if (commutor != null) {
            commutedOverloads = new HashSet<TOverload>();
            for (TCommutativeOverloads commutativeOverloads : finder.find(TCommutativeOverloads.class)) {
                commutativeOverloads.addTo(commutedOverloads);
            }
        }
        else {
            commutedOverloads = null;
        }
        Multimap<String, V> overloadsByName = ArrayListMultimap.create();

        int errors = 0;
        for (R scalar : finder.find(plainClass)) {
            try {
                V validated = validator.apply(scalar);

                String[] names = validated.registeredNames();
                for (int i = 0; i < names.length; ++i)
                    names[i] = names[i].toLowerCase();

                for (String name : names)
                    overloadsByName.put(name, validated);

                if ((commutedOverloads != null) && commutedOverloads.remove(scalar)) {
                    V commuted = commutor.apply(validated);
                    for (String name : names)
                        overloadsByName.put(name, commuted);
                }
            } catch (RuntimeException e) {
                rejectTOverload(scalar, e);
                ++errors;
            } catch (AssertionError e) {
                rejectTOverload(scalar, e);
                ++errors;
            }
        }
        if ((commutedOverloads != null) && (!commutedOverloads.isEmpty())) {
            logger.error("overload(s) were marked as commutative, but not found: {}", commutedOverloads);
            ++errors;
        }

        if (errors > 0) {
            StringBuilder sb = new StringBuilder("Found ").append(errors).append(" error");
            if (errors != 1)
                sb.append('s');
            sb.append(" while collecting scalar functions. Check logs for details.");
            throw new AkibanInternalException(sb.toString());
        }

        ArrayListMultimap<String, ScalarsGroup<V>> results = ArrayListMultimap.create();
        for (Map.Entry<String, Collection<V>> entry : overloadsByName.asMap().entrySet()) {
            String overloadName = entry.getKey();
            Collection<V> allOverloads = entry.getValue();
            for (Collection<V> priorityGroup : scalarsByPriority(allOverloads)) {
                ScalarsGroup<V> scalarsGroup = new ScalarsGroupImpl<V>(priorityGroup, castResolver);
                results.put(overloadName, scalarsGroup);
            }
        }
        results.trimToSize();
        return Multimaps.unmodifiableListMultimap(results);
    }

    private static <V extends TOverload> List<Collection<V>> scalarsByPriority(
            Collection<V> overloads)
    {
        // First, we'll put this into a SortedMap<Integer, Collection<TVO>> so that we have each subset of the
        // overloads grouped by priority. Then we'll go over those collections; for each one, we'll wrap it in
        // an unmodifiable Collection (so that users of the iterator() can't modify the Collections).
        // Finally, we'll wrap the result in an unmodifiable Collection (so that users can't remove Collections
        // from it via the Iterator).
        SortedMap<Integer, ArrayList<V>> byPriority = new TreeMap<Integer, ArrayList<V>>();
        for (V overload : overloads) {
            for (int priority : overload.getPriorities()) {
                ArrayList<V> thisPriorityOverloads = byPriority.get(priority);
                if (thisPriorityOverloads == null) {
                    thisPriorityOverloads = new ArrayList<V>();
                    byPriority.put(priority, thisPriorityOverloads);
                }
                thisPriorityOverloads.add(overload);
            }
        }

        List<Collection<V>> results = new ArrayList<Collection<V>>(byPriority.size());
        for (ArrayList<V> priorityGroup : byPriority.values()) {
            priorityGroup.trimToSize();
            results.add(Collections.unmodifiableCollection(priorityGroup));
        }
        return results;
    }

    private static <R> void rejectTOverload(R overload, Throwable e) {
        StringBuilder sb = new StringBuilder("rejecting overload ");
        Class<?> overloadClass = overload == null ? null : overload.getClass();
        try {
            sb.append(overload).append(' ');
        } catch (Exception e1) {
            logger.error("couldn't toString overload: " + overload);
        }
        sb.append("from ").append(overloadClass);
        logger.error(sb.toString(), e);
    }

    private final ListMultimap<String, ScalarsGroup<V>> overloadsByName;

    // class state
    private static final Logger logger = LoggerFactory.getLogger(ResolvablesRegistry.class);

    // static classes

    static final TClass differentTargetTypes
            = new NoAttrTClass(AkBundle.INSTANCE.id(), "differentTargets", null, null, 0, 0, 0, null, null, null);

    static final OverloadsFolder sameInputSets = new OverloadsFolder() {
        @Override
        protected TClass foldOne(TClass accumulated, TClass input) {
            return (accumulated == input) ? accumulated : differentTargetTypes;
        }
    };

    protected static class ScalarsGroupImpl<V extends TValidatedOverload> implements ScalarsGroup<V> {

        @Override
        public Collection<? extends V> getOverloads() {
            return overloads;
        }

        public ScalarsGroupImpl(Collection<V> overloads, final TCastResolver castResolver) {
            this.overloads = Collections.unmodifiableCollection(overloads);

            // Compute the same-type-ats
            // Tranform the map to a BitSet for efficiency
            OverloadsFolder.Result<TClass> sameTypeResults = sameInputSets.fold(overloads);
            this.sameTypeBitsetLen = sameTypeResults.finiteArityList().size();
            sameTypeBitSet = new BitSet(sameTypeBitsetLen);
            for (int i = 0; i < sameTypeBitsetLen; ++i) {
                if (sameTypeResults.finiteArityList().get(i) != differentTargetTypes)
                    sameTypeBitSet.set(i);
            }
            sameTypeVarargs = (sameTypeResults.infiniteArityElement(null) != differentTargetTypes);

            // compute common types
            commonTypes = new OverloadsFolder() {
                @Override
                protected TClass foldOne(TClass accumulated, TClass input) {
                    return castResolver.commonTClass(accumulated, input);
                }
            }.fold(overloads);
        }

        @Override
        public TClass commonTypeAt(int pos) {
            return commonTypes.at(pos, null);
        }

        @Override
        public boolean hasSameTypeAt(int pos)
        {
            return pos >= sameTypeBitsetLen
                    ? sameTypeVarargs
                    : sameTypeBitSet.get(pos);
        }

        private final int sameTypeBitsetLen;
        private final boolean sameTypeVarargs;
        private final BitSet sameTypeBitSet;
        private final OverloadsFolder.Result<TClass> commonTypes;
        private final Collection<? extends V> overloads;

    }

}
