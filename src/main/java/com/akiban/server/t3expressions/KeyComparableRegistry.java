
package com.akiban.server.t3expressions;

import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TKeyComparable;
import com.akiban.server.types3.service.InstanceFinder;

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
