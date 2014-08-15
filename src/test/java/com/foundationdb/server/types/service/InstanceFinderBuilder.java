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

import com.foundationdb.server.types.service.InstanceFinder;
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

    public <T> void put(Class<T> cls, Object... objects) {
        for (Object obj : objects) {
            instances.put(cls, cls.cast(obj));
        }
    }

    private Multimap<Class<?>,Object> instances = HashMultimap.create();
}
