package com.akiban.collation;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

/**
 * Provides Collator instances.  Collator is not threadsafe, so this class
 * keeps SoftReferences to thread-local instances.
 * 
 * @author peter
 *
 */
public class CollatorFactory {

    private final static Map<String, Collator> sourceMap = new HashMap<String, Collator>();

    private final static ThreadLocal<Map<String, SoftReference<Collator>>> cache = new ThreadLocal<Map<String, SoftReference<Collator>>>();
    
    /**
     * 
     * @param name
     * @return
     */
    public static Collator getCollator(final String name) {
        Map<String, SoftReference<Collator>> myMap = cache.get();
        if (myMap == null) {
            myMap = new HashMap<String, SoftReference<Collator>>();
            cache.set(myMap);
        }
        Collator collator = null;
        SoftReference<Collator> ref = myMap.get(name);
        if (ref != null) {
            collator = ref.get();
        }
        if (collator == null) {
            collator = sourceMap.get(name);
            if (collator == null) {
                collator = Collator.getInstance(new ULocale(name));
                if (collator == null) {
                    throw new IllegalArgumentException("No such Collator named: " + name);
                }
                collator.setStrength(1);
                sourceMap.put(name, collator);
            }
            collator = collator.cloneAsThawed();
            myMap.put(name, new SoftReference<Collator>(collator));
        }
        return collator;
    }
    
}
