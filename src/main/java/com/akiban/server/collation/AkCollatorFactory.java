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
import java.util.concurrent.ConcurrentHashMap;

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

    private final static Map<String, SoftReference<AkCollator>> collatorMap = new ConcurrentHashMap<String, SoftReference<AkCollator>>();

    public final static boolean MAP_CI = Boolean.parseBoolean(System.getProperty("akiban.collation.map_ci", "true"));

    /**
     * 
     * @param name
     * @return an AkCollator
     */
    public static AkCollator getAkCollator(final String name) {
        if (UCS_BINARY.equalsIgnoreCase(name) ||
        // TODO: Temporarily just know this one.
                !MAP_CI || !"latin1_swedish_ci".equals(name)) {
            return UCS_BINARY_COLLATOR;
        }

        SoftReference<AkCollator> ref = collatorMap.get(name);
        if (ref != null) {
            AkCollator akCollator = ref.get();
            if (akCollator != null) {
                return akCollator;
            }
        }
        /*
         * Note that another thread may win a race here, but it doesn't
         * matter. The result is that there will be an AkCollator in
         * the map which is sufficient.
         */
        AkCollator akCollator = new AkCollatorICU(name);
        collatorMap.put(name, new SoftReference<AkCollator>(akCollator));
        return akCollator;
    }

    /**
     * Construct an actual ICU Collator given a collation scheme name. The
     * result is a Collator that must be use in a thread-private manner.
     * 
     * @param name
     * @return
     */
    static synchronized Collator forName(final String name) {
        Collator collator = sourceMap.get(name);
        if (collator == null) {
            /*
             * TODO - figure out how ICU4J decodes names - this is certainly
             * wrong.
             */
            String locale = "sv_SE"; // Swedish for Sweden.
            int strength = Collator.SECONDARY; // _ci; _cs = TERTIARY.

            collator = Collator.getInstance(new ULocale(locale));
            if (collator == null) {
                throw new IllegalArgumentException("No such Collator named: " + name);
            }

            collator.setStrength(strength);

            sourceMap.put(name, collator);
        }
        collator = collator.cloneAsThawed();
        return collator;
    }

}
