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

package com.foundationdb.server.types.service;

import com.foundationdb.server.error.AkibanInternalException;
import com.foundationdb.server.error.OverloadException;
import com.foundationdb.server.types.InputSetFlags;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCommutativeOverloads;
import com.foundationdb.server.types.TOverload;
import com.foundationdb.server.types.aksql.AkBundle;
import com.foundationdb.server.types.common.types.NoAttrTClass;
import com.foundationdb.server.types.texpressions.TValidatedOverload;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Collections2;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

final class ResolvablesRegistry<V extends TValidatedOverload> implements Iterable<V> {

    public Iterable<? extends ScalarsGroup<V>> get(String name) {
        List<ScalarsGroup<V>> result = overloadsByName.get(name.toLowerCase());
        return result.isEmpty() ? null : result;
    }

    public Collection<? extends Map.Entry<String, ScalarsGroup<V>>> entriesByName() {
        return Collections.unmodifiableCollection(overloadsByName.entries());
    }

    public Collection<ScalarsGroup<V>> allScalarsGroups() {
        return Collections2.transform(
                entriesByName(),
                new Function<Map.Entry<String, ScalarsGroup<V>>, ScalarsGroup<V>>() {
                    @Override
                    public ScalarsGroup<V> apply(Map.Entry<String, ScalarsGroup<V>> input) {
                        return input.getValue();
                    }
                });
    }

    public boolean containsKey(String name) {
        return overloadsByName.containsKey(name.toLowerCase());
    }

    @Override
    public Iterator<V> iterator() {
        return new InternalIterator();
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
        return new ResolvablesRegistry<>(overloadsByName);
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
            commutedOverloads = new HashSet<>();
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
                ScalarsGroup<V> scalarsGroup = new ScalarsGroupImpl<>(priorityGroup, castResolver);
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
        SortedMap<Integer, ArrayList<V>> byPriority = new TreeMap<>();
        for (V overload : overloads) {
            for (int priority : overload.getPriorities()) {
                ArrayList<V> thisPriorityOverloads = byPriority.get(priority);
                if (thisPriorityOverloads == null) {
                    thisPriorityOverloads = new ArrayList<>();
                    byPriority.put(priority, thisPriorityOverloads);
                }
                thisPriorityOverloads.add(overload);
            }
        }

        List<Collection<V>> results = new ArrayList<>(byPriority.size());
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

    // inner classes

    private class InternalIterator implements Iterator<V> {
        @Override
        public boolean hasNext() {
            final boolean haveNext;
            if (withinGroupIterator != null && withinGroupIterator.hasNext()) {
                haveNext = true;
            }
            else {
                if (groupsIter.hasNext()) {
                    withinGroupIterator = groupsIter.next().getOverloads().iterator();
                    haveNext = withinGroupIterator.hasNext();
                    if (!haveNext)
                        withinGroupIterator = null;
                }
                else {
                    haveNext = false;
                }
            }
            return haveNext;
        }

        @Override
        public V next() {
            if (!hasNext()) // also advances to the next withinGroupIterator, if needed
                throw new NoSuchElementException();
            return withinGroupIterator.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        private InternalIterator() {
            this.groupsIter = allScalarsGroups().iterator();
            this.withinGroupIterator = null;
        }

        private Iterator<? extends ScalarsGroup<V>> groupsIter;
        private Iterator<? extends V> withinGroupIterator;
    }

    // class state
    private static final Logger logger = LoggerFactory.getLogger(ResolvablesRegistry.class);

    // static classes

    static final TClass differentTargetTypes
            = new NoAttrTClass(AkBundle.INSTANCE.id(), "differentTargets", null, null, 0, 0, 0, null, null, -1, null);

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
            sameTypeAt = sameTypeResults.toInputSetFlags(new Predicate<TClass>() {
                @Override
                public boolean apply(TClass input) {
                    return input != differentTargetTypes;
                }
            });

            // compute common types
            commonTypes = new OverloadsFolder() {
                @Override
                protected TClass foldOne(TClass accumulated, TClass input) {
                    if (accumulated == differentTargetTypes || input == differentTargetTypes)
                        return differentTargetTypes;
                    try {
                        return castResolver.commonTClass(accumulated, input);
                    }
                    catch (OverloadException e) {
                        return differentTargetTypes;
                    }
                }
            }
            .fold(overloads)
            .transform(new Function<TClass, TClass>() {
                @Override
                public TClass apply(TClass input) {
                    return input == differentTargetTypes ? null : input;
                }
            });
        }

        @Override
        public TClass commonTypeAt(int pos) {
            return commonTypes.at(pos, null);
        }

        @Override
        public boolean hasSameTypeAt(int pos)
        {
            return sameTypeAt.get(pos);
        }

        private final InputSetFlags sameTypeAt;
        private final OverloadsFolder.Result<TClass> commonTypes;
        private final Collection<? extends V> overloads;

    }
}
