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

package com.akiban.server.types3.mcompat.mcasts;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TComparison;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.google.common.primitives.Longs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.akiban.server.types3.pvalue.PValueSources.getLong;

public final class MKeyComparables {

    public static final TKeyComparable[] intComparisons = createIntComparisons();

    private static TKeyComparable[] createIntComparisons() {
        final TComparison integerComparison = new TComparison() {
            @Override
            public int compare(TInstance leftInstance, PValueSource left, TInstance rightInstance, PValueSource right) {
                return Longs.compare(getLong(left), getLong(right));
            }
        };

        List<TClass> candidates = Arrays.<TClass>asList(
                MNumeric.TINYINT, MNumeric.SMALLINT, MNumeric.MEDIUMINT, MNumeric.INT, MNumeric.BIGINT,
                MNumeric.TINYINT_UNSIGNED, MNumeric.SMALLINT_UNSIGNED, MNumeric.MEDIUMINT_UNSIGNED,
                MNumeric.INT_UNSIGNED
                // BIGINT UNSIGNED is not here, because its representation is not a signed long
        );
        Set<Set<TClass>> alreadyCreated = new HashSet<Set<TClass>>();
        List<TKeyComparable> results = new ArrayList<TKeyComparable>();
        for (TClass outer : candidates) {
            for (TClass inner : candidates) {
                if (inner == outer)
                    continue;
                Set<TClass> pair = new HashSet<TClass>(Arrays.asList(inner, outer));
                if (alreadyCreated.add(pair)) {
                    results.add(new TKeyComparable(outer, inner, integerComparison));
                }
            }
        }
        return results.toArray(new TKeyComparable[results.size()]);
    }
    
    
    private MKeyComparables() {}
}
