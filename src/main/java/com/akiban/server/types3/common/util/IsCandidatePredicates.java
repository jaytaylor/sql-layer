
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

        final Collection<TClass> asSet = new HashSet<>(tClasses.size());
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
