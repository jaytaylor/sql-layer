
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

    private static class StrongCastsGenerator extends TStrongCasts {

        @Override
        public Iterable<TCastIdentifier> get() {
            Set<TCastIdentifier> results = new HashSet<>(targets.length * sources.length);
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
