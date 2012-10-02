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

package com.akiban.server.types3;

import com.akiban.server.error.AkibanInternalException;
import com.google.common.base.Predicate;
import com.google.common.collect.ObjectArrays;

import java.util.HashSet;
import java.util.Set;

public abstract class TStrongCasts {

    public static TStrongCastsBuilder from(TClass firstSource, TClass... sources) {
        return new TStrongCastsBuilder(ObjectArrays.concat(sources, firstSource));
    }

    public abstract Iterable<TCastIdentifier> get();

    private TStrongCasts() { }

    public static class TStrongCastsBuilder {

        public TStrongCasts to(TClass firstTarget, TClass... targets) {
            return new StrongCastsGenerator(sources, ObjectArrays.concat(targets, firstTarget));
        }

        private TStrongCastsBuilder(TClass... sources) {
            assert sources.length > 0;
            this.sources = sources;
        }

        private final TClass[] sources;
    }

    public interface TClassPredicate extends Predicate<TClass> {}

    private static class StrongCastsGenerator extends TStrongCasts {

        @Override
        public Iterable<TCastIdentifier> get() {
            Set<TCastIdentifier> results = new HashSet<TCastIdentifier>(targets.length * sources.length);
            for (TClass source :sources) {
                for (TClass target : targets) {
                    TCastIdentifier identifier = new TCastIdentifier(source, target);
                    if (!results.add(identifier))
                        throw new AkibanInternalException("duplicate strong cast identifier: " + identifier);
                }
            }
            return results;
        }

        private StrongCastsGenerator(TClass[] sources, TClass[] targets) {
            this.sources = sources;
            this.targets = targets;
        }

        private final TClass[] sources;
        private final TClass[] targets;
    }
}
