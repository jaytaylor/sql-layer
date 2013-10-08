/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.foundationdb.server.types;

import com.foundationdb.server.error.AkibanInternalException;
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
