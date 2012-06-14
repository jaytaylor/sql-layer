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

package com.akiban.server.types.conversion;

import com.akiban.server.types.AkType;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

final class ConversionsBuilder {

    // ConversionsBuilder interface

    /**
     * Sets up a set of legal conversions. Note that this isn't defined as a "foo can convert to bar", but rather
     * as a "bar is convertible from foo."
     * @param targetType the type that will be converted to
     * @param sources the source type
     */
    public void legalConversions(AkType targetType, AkType... sources) {
        usable(targetType);
        for (AkType uncheckedSource : sources) {
            AkType source = usable(uncheckedSource);
            result.get(source).add(targetType);
        }
    }

    /**
     * Sets up an alias of types. The alias must not have any conversions defined other than its identity. From this
     * point on, the main type and alias will have exactly the same conversions.
     * @param mainType the main type
     * @param alias the type which is an alias of the main type
     */
    public void alias(AkType mainType, AkType alias) {
        if (result.get(alias).size() != 1 || !result.get(alias).contains(alias))
            throw new IllegalArgumentException("alias must only map to itself: " + alias + " -> " + result.get(alias));
        aliasesToMain.put(alias, mainType);
        result.remove(alias);
    }

    /**
     * Creates a ConversionsBuilder with certain types disallowed
     * @param disallowed the disallowed types
     */
    public ConversionsBuilder(AkType... disallowed) {
        this.disallowed = EnumSet.noneOf(AkType.class);
        Collections.addAll(this.disallowed, disallowed);
        // identities
        for (AkType type : AkType.values()) {
            if (!this.disallowed.contains(type))
                result.put(type, EnumSet.of(type));
        }
    }

    public Map<AkType,Set<AkType>> result() {
        Map<AkType,Set<AkType>> full = new EnumMap<AkType, Set<AkType>>(result);
        for (Map.Entry<AkType,AkType> entry : aliasesToMain.entrySet()) {
            AkType alias = entry.getKey();
            AkType main = entry.getValue();
            assert result.get(alias) == null : result.get(alias);
            // alias works like main
            full.put(alias, full.get(main));
            // anyone that's convertible from main must also be for alias
            for (Set<AkType> targets : result.values()) {
                if (targets.contains(main))
                    targets.add(alias);
            }
        }
        return full;
    }

    // private methods

    private AkType usable(AkType type) {
        if (disallowed.contains(type))
            throw new IllegalArgumentException(type + " is disallowed");
        if (aliasesToMain.containsKey(type))
            throw new IllegalArgumentException(type + " is aliased to " + aliasesToMain.get(type));
        return type;
    }

    // object state
    private final Map<AkType,Set<AkType>> result = new EnumMap<AkType, Set<AkType>>(AkType.class);
    private final Map<AkType,AkType> aliasesToMain = new EnumMap<AkType, AkType>(AkType.class);
    private final Set<AkType> disallowed;
}
