/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */

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
