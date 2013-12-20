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

package com.foundationdb.server.types.service;

import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TKeyComparable;
import com.foundationdb.server.types.service.InstanceFinder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class KeyComparableRegistry {

    public KeyComparableRegistry(InstanceFinder finder) {
        Collection<? extends TKeyComparable> keyComparables = finder.find(TKeyComparable.class);
        keyComparablesMap = new HashMap<>(keyComparables.size());
        for (TKeyComparable keyComparable : keyComparables) {
            TwoElemSet key = new TwoElemSet(keyComparable.getLeftTClass(), keyComparable.getRightTClass());
            keyComparablesMap.put(key, keyComparable);
        }
    }

    private final Map<TwoElemSet, TKeyComparable> keyComparablesMap;

    public TKeyComparable getClass(TClass left, TClass right) {
        return keyComparablesMap.get(new TwoElemSet(left, right));
    }

    private static final class TwoElemSet {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            TwoElemSet that = (TwoElemSet) o;

            if (a == that.a)
                return b == that.b;
            else
                return a == that.b && b == that.a;
        }

        @Override
        public int hashCode() {
            return a.hashCode() + b.hashCode();
        }

        private TwoElemSet(TClass a, TClass b) {
            this.a = a;
            this.b = b;
        }

        private final TClass a, b;
    }
}
