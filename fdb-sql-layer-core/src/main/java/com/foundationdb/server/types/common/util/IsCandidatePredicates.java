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

package com.foundationdb.server.types.common.util;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TPreptimeValue;
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
                    TInstance type = input.get(i).type();
                    if ((type != null) && (type.typeClass() == tClassFinal))
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
                        if (inputs.get(i).type() == null)
                            return false;
                    }
                    return true;
                }
            };

    public static Predicate<List<? extends TPreptimeValue>> containsOnly(Collection<? extends TClass> tClasses) {

        final Collection<TClass> asSet = new HashSet<>(tClasses.size());
        asSet.addAll(tClasses);
        return new Predicate<List<? extends TPreptimeValue>>() {
            @Override
            public boolean apply(List<? extends TPreptimeValue> inputs) {
                for (int i = 0, size = inputs.size(); i < size; ++i) {
                    TInstance type = inputs.get(i).type();
                    if (type == null || (!asSet.contains(type.typeClass())))
                        return false;
                }
                return true;
            }
        };
    }

    private IsCandidatePredicates() {}
}
