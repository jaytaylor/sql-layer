package com.akiban.cserver.service.session;

import com.akiban.util.ArgumentValidation;

import java.util.HashMap;
import java.util.Map;

public final class SessionImpl implements Session
{

    private final Map<Key,Object> map = new HashMap<Key,Object>();

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(String module, Object key) {
        return (T) map.get(new Key(module, key));
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T put(String module, Object key, T item) {
        return (T) map.put(new Key(module, key), item);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T remove(String module, Object key) {
        return (T) map.remove(new Key(module, key));
    }

    @Override
    public boolean isCanceled()
    {
        return false;
    }
    
    @Override
    public void close()
    {
        // For now do nothing.
        // Later, we'll close any "resource" that is added to the session.
    }
    
    private static class Key
    {
        private final String module;
        private final Object key;

        Key(String module, Object key) {
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
