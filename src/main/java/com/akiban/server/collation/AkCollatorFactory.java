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

package com.akiban.server.collation;

import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;

import com.ibm.icu.text.Collator;
import com.ibm.icu.util.ULocale;

/**
 * Provides Collator instances. Collator is not threadsafe, so this class keeps
 * SoftReferences to thread-local instances.
 * 
 * @author peter
 * 
 */
public class AkCollatorFactory {

    public final static String UCS_BINARY = "UCS_BINARY";

    public final static AkCollator UCS_BINARY_COLLATOR = new AkCollatorBinary();

    private final static Map<String, Collator> sourceMap = new HashMap<String, Collator>();

    private final static ThreadLocal<Map<String, SoftReference<AkCollator>>> cache = new ThreadLocal<Map<String, SoftReference<AkCollator>>>();

    public final static boolean MAP_CI = Boolean.getBoolean("akiban.collation.map_ci");

    /**
     * 
     * @param name
     * @return
     */
    public static AkCollator getCollator(final String name) {
        if (UCS_BINARY.equalsIgnoreCase(name) ||
            // TODO: Temporarily just know this one.
            !MAP_CI || !"latin1_swedish_ci".equals(name)) {
            return UCS_BINARY_COLLATOR;
        }

        Map<String, SoftReference<AkCollator>> myMap = cache.get();
        if (myMap == null) {
            myMap = new HashMap<String, SoftReference<AkCollator>>();
            cache.set(myMap);
        } else {
            SoftReference<AkCollator> ref = myMap.get(name);
            if (ref != null) {
                return ref.get();
            }
        }
        Collator collator = sourceMap.get(name);
        if (collator == null) {
            /*
             * TODO  - figure out how ICU4J decodes names - this is certainly wrong.
             */
            String locale = "sv_SV";
            int strength = Collator.SECONDARY; // _ci; _cs = TERTIARY.

            collator = Collator.getInstance(new ULocale(locale));
            if (collator == null) {
                throw new IllegalArgumentException("No such Collator named: " + name);
            }
            
            collator.setStrength(strength);
            
            sourceMap.put(name, collator);
        }
        collator = collator.cloneAsThawed();
        AkCollator akCollator = new AkCollatorICU(collator);
        myMap.put(name, new SoftReference<AkCollator>(akCollator));
        return akCollator;
    }

}
