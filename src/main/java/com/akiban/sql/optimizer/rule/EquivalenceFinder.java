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

package com.akiban.sql.optimizer.rule;

import com.akiban.util.ArgumentValidation;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class EquivalenceFinder<T> {

    public void copyEquivalences(EquivalenceFinder<? extends T> source) {
        equivalences.putAll(source.equivalences);
    }

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

    /**
     * <p>Return a set of all of the equivalence pairs defined in this instance, with reflected-duplicates removed.
     * For instance, rather than a pair <code>(u, v)</code> and another pair <code>(v, u)</code>, this set will contain
     * either <code>(u, v)</code> <em>or</em> <code>(v, u)</code>, but not both.</p>
     *
     * <p>The resulting set is returned as a <code>Map.Entry</code>, but "key" and "value" are arbitrary in this
     * context.</p>
     * @return the equivalence entries
     */
    public Set<Entry<T, T>> equivalencePairs() {
        Collection<Entry<T, T>> entries = equivalences.entries();
        Map<T,T> normalized = new HashMap<T, T>(entries.size() / 2);
        for (Entry<T,T> entry : entries) {
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
        return normalized.entrySet();
    }

    public Set<T> findParticipants() {
        Set<Entry<T,T>> pairs = equivalencePairs();
        Set<T> results = new HashSet<T>(pairs.size() * 2);
        for (Entry<T,T> pair : pairs) {
            results.add(pair.getKey());
            results.add(pair.getValue());
        }
        return results;
    }

    @Override
    public String toString() {
        Set<Entry<T, T>> normalizedEntries = equivalencePairs();

        StringBuilder sb = new StringBuilder(EquivalenceFinder.class.getSimpleName()).append('{');
        for (Iterator<Entry<T, T>> iterator = normalizedEntries.iterator(); iterator.hasNext(); ) {
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
