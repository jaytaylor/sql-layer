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
package com.foundationdb.direct;

import java.util.HashMap;
import java.util.Map;

import com.foundationdb.ais.model.AkibanInformationSchema;
import com.foundationdb.ais.model.CacheValueGenerator;
import com.foundationdb.sql.embedded.JDBCConnection;

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
            o = newInstance(c);
            if (o != null) {
                instanceMap.get().put(c, o);
            }
        }
        return o;
    }
    
    /**
     * Construct a new instance of an entity object of the given type.
     * @param c Type (the interface class) of object
     * @return A newly constructed implementation object
     */
    public static AbstractDirectObject newInstance(final Class<?> c) {
        try {
            Class<? extends AbstractDirectObject> cl = classMap.get(c);
            return cl.newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassCastException e) {
            throw new RuntimeException(e);
        }
        
    }
    
    public static void enter(final String schemaName, AkibanInformationSchema ais) {
        DirectClassLoader dcl = ais.getCachedValue(CACHE_KEY, new CacheValueGenerator<DirectClassLoader>() {

            @Override
            public DirectClassLoader valueFor(AkibanInformationSchema ais) {
                return new DirectClassLoader(getClass().getClassLoader(), schemaName, ais);
            }
            
        });
        
        DirectContextImpl parent = contextThreadLocal.get();
        DirectContextImpl dc = new DirectContextImpl(schemaName, dcl, parent);
        contextThreadLocal.set(dc);
        dc.enter();
    }
    
    public static DirectContextImpl getContext() {
        return contextThreadLocal.get();
    }
    
    public static void leave() {
        DirectContextImpl dc = contextThreadLocal.get();
        assert dc != null : "enter() was not called before leave()";
        dc.leave();
        DirectContextImpl parent = dc.getParent();
        if (parent == null)
            contextThreadLocal.remove();
        else
            contextThreadLocal.set(parent);
    }

}
