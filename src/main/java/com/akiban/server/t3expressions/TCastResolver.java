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

import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public final class TCastResolver {

    private final TCastsRegistry castsRegistry;

    public TCastResolver(TCastsRegistry castsRegistry) {
        this.castsRegistry = castsRegistry;
    }

    public TCast cast(TInstance source, TInstance target) {
        return castsRegistry.cast(source.typeClass(), target.typeClass());
    }

    Collection<Map<TClass, TCast>> castsBySource() {
        return castsRegistry.castsBySource();
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
        Set<? extends TClass> t1Targets = stronglyCastableFrom(tClass1);
        Set<? extends TClass> t2Targets = stronglyCastableFrom(tClass2);

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

    /**
     * Find the registered cast going from source to taret.
     * @param source Type to cast from
     * @param target Type to cast to
     * @return Return matching cast or <tt>null</tt> if none
     */
    public TCast cast(TClass source, TClass target) {
        return castsRegistry.cast(source, target);
    }

    public Set<TClass> stronglyCastableFrom(TClass tClass) {
        return castsRegistry.stronglyCastableFrom(tClass);
    }

    public boolean strongCastExists(TClass source, TClass target) {
        return isStrong(castsRegistry.cast(source, target));
    }

    public boolean isMostSpecific(TClass candidate, Set<? extends TClass> castGroup) {
        for (TClass inner : castGroup) {
            if (candidate.equals(inner))
                continue;
            if (!stronglyCastable(candidate, inner)) {
                return false;
            }
        }
        return true;
    }

    public boolean isIndexFriendly(TClass source, TClass target) {
        return Objects.equal(
                source.name().categoryName(),
                target.name().categoryName()
        );
    }

    public boolean stronglyCastable(TClass source, TClass target) {
        return isStrong(castsRegistry.cast(source, target));
    }

    boolean isStrong(TCast cast) {
        return (cast != null) && castsRegistry.isStrong(cast);
    }
}
