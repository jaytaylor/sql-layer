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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class EquivalenceFinder<T> {
    
    public void markEquivalent(T one, T two) {
        ArgumentValidation.notNull("first arg", one);
        ArgumentValidation.notNull("second arg", two);
        if (one.equals(two))
            return; // equals implies equivalence even without this
        
        int hashOne = one.hashCode();
        int hashTwo = two.hashCode();
        if (hashOne < hashTwo) {
            equivalences.put(one, two);
        }
        else if (hashOne > hashTwo) {
            equivalences.put(two, one);
        }
        else {
            equivalences.put(one, two);
            equivalences.put(two, one);
        }
    }

    public boolean areEquivalent(T one, T two) {
        return areEquivalent(one, two, maxTraversals, freshHashSet());
    }

    // for testing
    void tooMuchTraversing() {
    }
    
    private boolean areEquivalent(T one, T two, int remainingTraversals, Set<T> seenItems) {
        if (one.equals(two))
            return true;
        if (--remainingTraversals < 0) {
            tooMuchTraversing();
            return false;
        }
        if (!seenItems.add(one))
            return false;
        
        int hashOne = one.hashCode();
        int hashTwo = two.hashCode();
        if (hashOne < hashTwo) {
            return findEquivalence(one, two, remainingTraversals, seenItems);
        }
        else if (hashOne > hashTwo) {
            return findEquivalence(two, one, remainingTraversals, seenItems);
        }
        else {
            return one.equals(two)
                    || findEquivalence(one, two, remainingTraversals, seenItems)
                    || findEquivalence(two, one, remainingTraversals, seenItems);
        }
    }
    
    private boolean findEquivalence(T one, T two, int remainingTraversals, Set<T> seenItems) {
        assert one.hashCode() <= two.hashCode();
        Collection<T> oneEquivalents = equivalences.get(one);
        if (oneEquivalents.contains(two)) // special case, since the collection is a set so this can be very speedy
            return true;
        for (T equivalent : oneEquivalents) {
            if (areEquivalent(equivalent, two, remainingTraversals, seenItems))
                return true;
        }
        return false;
    }

    private HashSet<T> freshHashSet() {
        // TODO cache this and return a cleared copy. not thread safe, but doesn't need to be, and saves on alloc
        return new HashSet<T>();
    }

    EquivalenceFinder() {
        this(16);
    }

    EquivalenceFinder(int maxTraversals) {
        this.maxTraversals = maxTraversals;
    }

    /**
     * Mapping of equivalences, one node at a time. To save a bit of space (both in puts and reads), for any two
     * equivalent nodes n1 and n2, if they have different hashCodes, then we only put in the key n1 with value n2.
     * If they have the same hash code, we have no option but to put them both in.
     */
    Multimap<T,T> equivalences = HashMultimap.create();
    private int maxTraversals;
}
