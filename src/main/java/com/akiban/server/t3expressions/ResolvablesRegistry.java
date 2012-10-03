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
import com.akiban.server.types3.TResolvable;
import com.akiban.server.types3.service.InstanceFinder;
import com.akiban.server.types3.texpressions.TValidatedResolvable;
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

final class ResolvablesRegistry<V extends TValidatedResolvable> {

    public Iterable<? extends ScalarsGroup<V>> get(String name) {
        List<ScalarsGroup<V>> result = overloadsByName.get(name.toLowerCase());
        return result.isEmpty() ? null : result;
    }

    public Collection<? extends Map.Entry<String, ScalarsGroup<V>>> entriesByName() {
        return Collections.unmodifiableCollection(overloadsByName.entries());
    }

    public static <R extends TResolvable, V extends TValidatedResolvable>
    ResolvablesRegistry<V> create(InstanceFinder finder,
                                  Class<R> plainClass,
                                  Function<R, V> validator,
                                  Function<V, V> commutor)
    {
        ListMultimap<String, ScalarsGroup<V>> overloadsByName = createScalars(finder, plainClass, validator, commutor);
        return new ResolvablesRegistry<V>(overloadsByName);
    }

    ResolvablesRegistry(ListMultimap<String, ScalarsGroup<V>> overloadsByName) {
        this.overloadsByName = overloadsByName;
    }

    private static <R extends TResolvable, V extends TValidatedResolvable>
    ListMultimap<String, ScalarsGroup<V>> createScalars(InstanceFinder finder,
                                                        Class<R> plainClass,
                                                        Function<R, V> validator,
                                                        Function<V, V> commutor)
    {

        Set<TResolvable> commutedOverloads;
        if (commutor != null) {
            commutedOverloads = new HashSet<TResolvable>();
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
                ScalarsGroup<V> scalarsGroup = new ScalarsGroupImpl<V>(priorityGroup);
                results.put(overloadName, scalarsGroup);
            }
        }
        results.trimToSize();
        return Multimaps.unmodifiableListMultimap(results);
    }

    private static <V extends TResolvable> List<Collection<V>> scalarsByPriority(
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

    protected static class ScalarsGroupImpl<V extends TValidatedResolvable> implements ScalarsGroup<V> {

        @Override
        public Collection<? extends V> getOverloads() {
            return overloads;
        }

        public ScalarsGroupImpl(Collection<V> overloads) {
            this.overloads = Collections.unmodifiableCollection(overloads);
            boolean outRange[] = new boolean[1];
            int argc[] = new int[1];
            sameType = doFindSameType(overloads, argc, outRange);

            outOfRangeVal = outRange[0];
            nArgs = argc[0];
        }

        @Override
        public boolean hasSameTypeAt(int pos)
        {
            return pos >= nArgs         // if pos is out of range
                    ? outOfRangeVal
                    : sameType.get(pos);
        }

        private static <V extends TValidatedResolvable> int nArgsOf(V ovl)
        {
            return ovl.positionalInputs()
                    + (ovl.varargInputSet() == null ? 0 : 1);
        }

        private static <V extends TValidatedResolvable> boolean hasVararg(V ovl)
        {
            return ovl.varargInputSet() != null;
        }

        protected static <V extends TValidatedResolvable>
        BitSet doFindSameType(Collection<? extends V> overloads, int range[], boolean outRange[])
        {
            ArrayList<Integer> nArgs = new ArrayList<Integer>();

            // overload with the longest argument list
            V maxOvl = overloads.iterator().next();
            int maxArgc = nArgsOf(maxOvl);
            boolean hasVararg = hasVararg(maxOvl);
            int n = 1;

            // whether arg that's out of range should be 'same' or 'not same'
            // false if all overloads in the group have fixed length
            // true if all overloads with varargs in the group have the same targetType of vararg
            // false otherwise
            boolean outOfRange = false;
            TClass firstVararg = null;

            for (V ovl : overloads)
            {
                int curArgc = nArgsOf(ovl);
                nArgs.add(curArgc);
                boolean curHasVar = hasVararg(ovl);

                if (curHasVar)
                {
                    if (firstVararg == null)
                    {
                        outOfRange = true;
                        firstVararg = ovl.varargInputSet().targetType();
                    }
                    else
                        outOfRange &= firstVararg.equals(ovl.varargInputSet().targetType());
                }

                if (curArgc > maxArgc
                        || curArgc == maxArgc
                        && hasVararg
                        && !curHasVar)
                {
                    hasVararg = hasVararg(ovl);
                    maxOvl = ovl;
                    maxArgc = curArgc;
                }
                ++n;
            }

            outRange[0] = outOfRange;
            range[0] = maxArgc;

            // all the overloads in the group are vararg
            if (hasVararg && maxArgc == 1)
            {
                BitSet ret = new BitSet(1);
                ret.set(0, outOfRange);
                return ret;
            }

            BitSet sameType = new BitSet(maxArgc);

            Boolean same;
            for (n = 0; n < maxArgc; ++n)
            {
                same = Boolean.TRUE;
                TClass common = maxOvl.inputSetAt(n).targetType();
                int index = 0;
                for (V ovl : overloads)
                {
                    int curArgc = nArgs.get(index++);
                    TClass targetType;

                    // if the arg is absent
                    if (n >= curArgc)
                    {
                        if (!hasVararg(ovl))
                            continue;
                        else
                            targetType = ovl.varargInputSet().targetType();
                    }
                    else
                        targetType = ovl.inputSetAt(n).targetType();

                    if (targetType != null && !targetType.equals(common)
                            || targetType == null && common != null)
                    {
                        same = Boolean.FALSE;
                        break;
                    }
                }
                sameType.set(n, same);
            }

            return sameType;
        }
        private final int nArgs;
        private final boolean outOfRangeVal;
        private final BitSet sameType;
        private final Collection<? extends V> overloads;

    }

}
