/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
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
package com.akiban.direct;

import java.util.HashMap;
import java.util.Map;

import com.akiban.ais.model.AkibanInformationSchema;
import com.akiban.ais.model.CacheValueGenerator;

/**
 * TODO - Total hack that this is static - need to a way to get this into the
 * context for JDBCResultSet.
 * 
 * @author peter
 * 
 */
public class Direct {

    private static final Object CACHE_KEY = new Object();

    private final static Map<Class<?>, Class<? extends AbstractDirectObject>> classMap = new HashMap<>();
    private final static ThreadLocal<Map<Class<?>, AbstractDirectObject>> instanceMap = new ThreadLocal<Map<Class<?>, AbstractDirectObject>>() {

        @Override
        protected Map<Class<?>, AbstractDirectObject> initialValue() {
            return new HashMap<Class<?>, AbstractDirectObject>();
        }
    };
    
    private final static ThreadLocal<DirectContextImpl> contextThreadLocal = new ThreadLocal<>();

    public static void registerDirectObjectClass(final Class<?> iface, final Class<? extends AbstractDirectObject> impl) {
        classMap.put(iface, impl);
    }

    /**
     * TODO - for now this clears everything!
     */

    public static void unregisterDirectObjectClasses() {
        classMap.clear();
        instanceMap.remove();

    }

    /**
     * Return a thread-private instance of an entity object of the registered
     * for a given Row, or null if there is none.
     */
    public static AbstractDirectObject objectForRow(final Class<?> c) {
        AbstractDirectObject o = instanceMap.get().get(c);
        if (o == null) {
            try {
                Class<? extends AbstractDirectObject> cl = classMap.get(c);
                o = cl.newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
                throw new RuntimeException(e);
            }
            if (o != null) {
                instanceMap.get().put(c, o);
            }
        }
        return o;
    }
    
    public static void enter(final String schemaName, AkibanInformationSchema ais) {
        DirectClassLoader dcl = ais.getCachedValue(CACHE_KEY, new CacheValueGenerator<DirectClassLoader>() {

            @Override
            public DirectClassLoader valueFor(AkibanInformationSchema ais) {
                return new DirectClassLoader(getClass().getClassLoader(), schemaName, ais);
            }
            
        });
        
        final DirectContextImpl dc = new DirectContextImpl(schemaName, dcl);
        contextThreadLocal.set(dc);
        dc.enter();
    }
    
    public static DirectContextImpl getContext() {
        return contextThreadLocal.get();
    }
    
    public static void leave() {
        DirectContextImpl dc = contextThreadLocal.get();
        contextThreadLocal.remove();
        assert dc != null : "enter() was not called before leave()";
        dc.leave();
    }

}
