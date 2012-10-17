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

package com.akiban.server.types3.common.util;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TPreptimeValue;
import com.google.common.base.Predicate;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public final class IsCandidatePredicates {

    public static Predicate<List<? extends TPreptimeValue>> contains(TClass tClass) {
        final TClass tClassFinal = tClass;
        return new Predicate<List<? extends TPreptimeValue>>() {
            @Override
            public boolean apply(List<? extends TPreptimeValue> input) {
                for (int i = 0, size=input.size(); i < size; ++i) {
                    TInstance tInstance = input.get(i).instance();
                    if ((tInstance != null) && (tInstance.typeClass() == tClassFinal))
                        return true;
                }
                return false;
            }
        };
    }

    public static Predicate<List<? extends TPreptimeValue>> allTypesKnown =
            new Predicate<List<? extends TPreptimeValue>>() {
                @Override
                public boolean apply(List<? extends TPreptimeValue> inputs) {
                    for (int i = 0, size=inputs.size(); i < size; ++i) {
                        if (inputs.get(i).instance() == null)
                            return false;
                    }
                    return true;
                }
            };

    public static Predicate<List<? extends TPreptimeValue>> containsOnly(Collection<? extends TClass> tClasses) {

        final Collection<TClass> asSet = new HashSet<TClass>(tClasses.size());
        asSet.addAll(tClasses);
        return new Predicate<List<? extends TPreptimeValue>>() {
            @Override
            public boolean apply(List<? extends TPreptimeValue> inputs) {
                for (int i = 0, size = inputs.size(); i < size; ++i) {
                    TInstance tInstance = inputs.get(i).instance();
                    if (tInstance == null || (!asSet.contains(tInstance.typeClass())))
                        return false;
                }
                return true;
            }
        };
    }

    private IsCandidatePredicates() {}
}
