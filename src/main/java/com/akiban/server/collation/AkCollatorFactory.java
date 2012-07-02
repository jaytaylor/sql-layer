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
import java.util.Properties;
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

    public final static boolean COLLATION_ENABLED = Boolean.parseBoolean(System.getProperty("akiban.collation.enabled",
            "true"));

    private final static String DEFAULT_PROPERTIES_FILE_NAME = "collation_names.properties";

    private final static String COLLATION_PROPERTIES_FILE_NAME_PROPERTY = "akiban.collation.properties";

    private final static Properties collationNameProperties = new Properties();

    private final static long SANITY_RELOAD_INTERVAL = 10000;

    private static volatile long lastReloadTime = Long.MIN_VALUE;

    /**
     * 
     * @param name
     * @return an AkCollator
     */
    public static AkCollator getAkCollator(final String name) {
        if (!COLLATION_ENABLED || name == null || UCS_BINARY.equalsIgnoreCase(name)) {
            return UCS_BINARY_COLLATOR;
        }
        
        final String scheme = schemeForName(name);
        if (scheme.startsWith(UCS_BINARY)) {
            return UCS_BINARY_COLLATOR;
        }

        SoftReference<AkCollator> ref = collatorMap.get(scheme);
        if (ref != null) {
            AkCollator akCollator = ref.get();
            if (akCollator != null) {
                return akCollator;
            }
        }
        /*
         * Note that another thread may win a race here, but it doesn't matter.
         * The result is that there will be an AkCollator in the map which is
         * sufficient.
         */
        AkCollator akCollator = new AkCollatorICU(name, scheme);
        collatorMap.put(name, new SoftReference<AkCollator>(akCollator));
        return akCollator;
    }

    /**
     * Construct an actual ICU Collator given a collation scheme name. The
     * result is a Collator that must be use in a thread-private manner.
     * 
     * Collation scheme names must be defined in a properties file, for which
     * the default location is
     * 
     * <pre>
     * <code>
     * com.akiban.server.collation.collation_names.properties
     * </code>
     * </pre>
     * 
     * This location can be overridden with the system property named
     * 
     * <pre>
     * <code>
     * akiban.collation.properties
     * </code>
     * </pre>
     * 
     * To support experimentation with new names this method will attempt
     * to reload the properties file whenever asked to find a collation name
     * that does not exist. Reloading is limited to once every 10 seconds for
     * avoid a avenue for denial-of-service.
     * 
     * @param scheme
     * @return
     */
    static synchronized Collator forScheme(final String scheme) {
        Collator collator = sourceMap.get(scheme);
        if (collator == null) {
            final String locale;
            final int strength;

            try {
                String[] pieces = scheme.split(",");
                locale = pieces[0];
                strength = Integer.parseInt(pieces[1]);
            } catch (Exception e) {
                throw new IllegalStateException("Malformed property for name " + scheme);
            }

            collator = Collator.getInstance(new ULocale(locale));
            if (collator == null) {
                throw new IllegalArgumentException("No such Collator named: " + scheme);
            }
            collator.setStrength(strength);
            sourceMap.put(scheme, collator);
        }
        collator = collator.cloneAsThawed();
        return collator;
    }

    private static String schemeForName(final String name) {
        final String lcname = name.toLowerCase();
        String scheme = collationNameProperties.getProperty(lcname);
        if (scheme == null) {
            reloadCollationProperties();
            scheme = collationNameProperties.getProperty(lcname);
            if (scheme == null) {
                throw new IllegalArgumentException("Collation " + name + " is unknown");
            }
        }
        return scheme;
    }
    
    
    private static void reloadCollationProperties() {
        long now = System.currentTimeMillis();
        if (now - SANITY_RELOAD_INTERVAL > lastReloadTime) {
            lastReloadTime = now;
            try {
                final String resourceName = System.getProperty(COLLATION_PROPERTIES_FILE_NAME_PROPERTY,
                        DEFAULT_PROPERTIES_FILE_NAME);
                collationNameProperties.clear();
                collationNameProperties.load(AkCollatorFactory.class.getResourceAsStream(resourceName));
            } catch (Exception e) {

            }
        }
    }


}
