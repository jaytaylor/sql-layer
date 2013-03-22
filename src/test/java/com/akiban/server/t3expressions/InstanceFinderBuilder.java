
package com.akiban.server.t3expressions;

import com.akiban.server.types3.service.InstanceFinder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import java.util.Collection;

class InstanceFinderBuilder implements InstanceFinder {
    @Override
    @SuppressWarnings("unchecked")
    public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
        Collection<?> resultWild = instances.get(targetClass);
        return (Collection<? extends T>) resultWild;
    }

    public <T> void put(Class<T> cls, T... objects) {
        for (Object obj : objects) {
            instances.put(cls, cls.cast(obj));
        }
    }

    private Multimap<Class<?>,Object> instances = HashMultimap.create();
}
