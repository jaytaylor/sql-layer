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

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class TStrongCasts {

    public static TStrongCastsBuilder from(TClass source) {
        return new TStrongCastsBuilder(source);
    }

    public abstract Iterable<TCastIdentifier> get(Iterable<? extends TClass> knownClasses);

    private TStrongCasts() { }

    public static class TStrongCastsBuilder {

        public TStrongCasts to(TClass firstTarget, TClass... targets) {
            List<TCastIdentifier> ids = new ArrayList<TCastIdentifier>();
            ids.add(new TCastIdentifier(source, firstTarget));
            for (TClass target : targets) {
                ids.add(new TCastIdentifier(source, target));
            }
            return new ExplicitStrongCasts(ids);
        }

        public TStrongCasts toAll(TBundleID inBundle) {
            return new ToAllCastsBuilder(source, inBundle);
        }

        private TStrongCastsBuilder(TClass source) {
            this.source = source;
        }

        private final TClass source;
    }

    private static class ExplicitStrongCasts extends TStrongCasts {

        @Override
        public Iterable<TCastIdentifier> get(Iterable<? extends TClass> knownClasses) {
            return iterable;
        }

        private ExplicitStrongCasts(Collection<TCastIdentifier> casts) {
            this.iterable = Collections.unmodifiableCollection(casts);
        }

        private final Collection<TCastIdentifier> iterable;
    }

    private static class ToAllCastsBuilder extends TStrongCasts {

        @Override
        public Iterable<TCastIdentifier> get(Iterable<? extends TClass> knownClasses) {
            return Iterables.transform(
                    Iterables.filter(knownClasses, inBundle),
                    castIdentifier);
        }

        private ToAllCastsBuilder(final TClass source, final TBundleID targetBundle) {
            this.inBundle = new Predicate<TClass>() {
                @Override
                public boolean apply( TClass input) {
                    return input.name().bundleId() == targetBundle;
                }
            };
            this.castIdentifier = new Function<TClass, TCastIdentifier>() {
                @Override
                public TCastIdentifier apply(TClass input) {
                    return new TCastIdentifier(source, input);
                }
            };
        }

        private final Predicate<TClass> inBundle;
        private final Function<TClass,TCastIdentifier> castIdentifier;
    }

}
