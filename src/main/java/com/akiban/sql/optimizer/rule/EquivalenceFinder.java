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

package com.akiban.sql.optimizer.rule;

import com.akiban.util.ArgumentValidation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class EquivalenceFinder<T> {

    public void markEquivalent(T one, T two) {
        ArgumentValidation.notNull("first arg", one);
        ArgumentValidation.notNull("second arg", two);
        if (one.equals(two))
            return; // equals implies equivalence even without this

        equivalences.put(one, two);
        equivalences.put(two, one);
    }

    public boolean areEquivalent(T one, T two) {
        initSeenItems();
        return areEquivalent(one, two, maxTraversals);
    }
    
    public Set<T> findEquivalents(T node) {
        Set<T> accumulator = new HashSet<T>();
        buildEquivalents(node, accumulator);
        boolean removedFirst = accumulator.remove(node);
        assert removedFirst : "didn't remove " + node + " from " + accumulator;
        return accumulator;
    }

    @Override
    public String toString() {
        Collection<Entry<T, T>> entries = equivalences.entries();
        
        // create a map which contains a "normalized" view of the equivalencies; each reflected pair is shown
        // only once. So rather than (u, v) and (v, u), it'll contain either (u, v) *or* (v, u) -- but not both.
        Map<T,T> normalized = new HashMap<T, T>(entries.size() / 2);
        for (Map.Entry<T,T> entry : entries) {
            T key = entry.getKey();
            T val = entry.getValue();
            boolean sawReflection;
            if (normalized.containsKey(val)) {
                T otherKey = normalized.get(val);
                if (otherKey == null)
                    sawReflection = (key == null);
                else
                    sawReflection = otherKey.equals(key);
            }
            else {
                sawReflection = false;
            }
            if (!sawReflection)
                normalized.put(key, val);
        }

        StringBuilder sb = new StringBuilder(EquivalenceFinder.class.getSimpleName()).append('{');
        for (Iterator<Entry<T, T>> iterator = normalized.entrySet().iterator(); iterator.hasNext(); ) {
            Entry<T, T> entry = iterator.next();
            String first = elementToString(entry.getKey());
            String second = elementToString(entry.getValue());
            sb.append('(').append(first).append(" = ").append(second).append(')');
            if (iterator.hasNext())
                sb.append(", ");
        }
        return sb.append('}').toString();
    }
    
    protected String describeNull() {
        return "null";
    }

    protected String describeElement(T element) {
        return element.toString();
    }

    // for testing
    void tooMuchTraversing() {
    }
    
    private String elementToString(T element) {
        return element == null ? describeNull() : describeElement(element);
    }
    
    private boolean areEquivalent(T one, T two, int remainingTraversals) {
        // base case
        if (one.equals(two))
            return true;
        // limit on search space
        if (--remainingTraversals < 0) {
            tooMuchTraversing();
            return false;
        }

        // we've seen this edge before, so don't need to traverse it again
        if (!seenNodes.add(one))
            return false;

        // recurse
        Collection<T> oneEquivalents = equivalences.get(one);
        // we do two quick-pass checks which take advantage of the fact that the collection is a Set with quick lookups
        if (oneEquivalents.isEmpty())
            return false;
        if (oneEquivalents.contains(two))
            return true;
        for (T equivalent : oneEquivalents) {
            if (areEquivalent(equivalent, two, remainingTraversals))
                return true;
        }
        return false;
    }

    private void buildEquivalents(T node, Set<T> accumulator) {
        if (!accumulator.add(node))
            return;
        for (T equivalence : equivalences.get(node)) {
            buildEquivalents(equivalence, accumulator);
        }
    }

    private void initSeenItems() {
        if (seenNodes == null)
            seenNodes = new HashSet<T>();
        else
            seenNodes.clear();
    }

    public EquivalenceFinder() {
        this(32);
    }

    EquivalenceFinder(int maxTraversals) {
        this.maxTraversals = maxTraversals;
    }

    /**
     * Mapping of equivalences, one node at a time. To save a bit of space (both in puts and reads), for any two
     * equivalent nodes n1 and n2, if they have different hashCodes, then we only put in the key n1 with value n2.
     * If they have the same hash code, we have no option but to put them both in.
     */
    private Multimap<T,T> equivalences = HashMultimap.create();
    private int maxTraversals;
    private Set<T> seenNodes;
}
