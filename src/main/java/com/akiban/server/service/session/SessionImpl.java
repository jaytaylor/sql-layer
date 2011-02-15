/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.server.service.session;

import java.util.HashMap;
import java.util.Map;

import com.akiban.util.ArgumentValidation;

public final class SessionImpl implements Session
{

    private final Map<Key,Object> map = new HashMap<Key,Object>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(Class<?> module, Object key) {
        return (T) map.get(new Key(module, key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T put(Class<?> module, Object key, T item) {
        return (T) map.put(new Key(module, key), item);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T remove(Class<?> module, Object key) {
        return (T) map.remove(new Key(module, key));
    }

    @Override
    public void close()
    {
        // For now do nothing to any cached resources.
        // Later, we'll close any "resource" that is added to the session.
        //
        map.clear();
    }
    
    private static class Key
    {
        private final Class<?> module;
        private final Object key;

        Key(Class<?> module, Object key) {
            ArgumentValidation.notNull("module", module);
            this.module = module;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Key key1 = (Key) o;

            return !(key != null ? !key.equals(key1.key) : key1.key != null) && module.equals(key1.module);

        }

        @Override
        public int hashCode() {
            int result = module.hashCode();
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }
    }
}
