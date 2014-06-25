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

package com.foundationdb.server.collation;

import java.io.IOException;
import java.lang.ref.SoftReference;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.foundationdb.server.error.UnsupportedCollationException;
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

    private final static Pattern SCHEMA_PATTERN = Pattern.compile("(\\d+):(\\w+(?:,\\d+)?)");

    public final static int MAX_COLLATION_ID = 126;

    public final static String UCS_BINARY = "UCS_BINARY";

    public final static String MYSQL = "mysql_";

    public final static AkCollator UCS_BINARY_COLLATOR = new AkCollatorBinary();

    private final static Map<String, Collator> sourceMap = new HashMap<>();

    private final static Map<String, SoftReference<AkCollator>> collatorMap = new ConcurrentHashMap<>();

    private final static Map<Integer, SoftReference<AkCollator>> collationIdMap = new ConcurrentHashMap<>();

    private final static String DEFAULT_PROPERTIES_FILE_NAME = "collation_data.properties";

    private final static String COLLATION_PROPERTIES_FILE_NAME_PROPERTY = "foundationdb.collation.properties";

    private final static Properties collationNameProperties = new Properties();

    private volatile static Mode mode = Mode.STRICT;

    /*
     * Note: used only in a single-threaded unit test.
     */
    private static int cacheHits;

    private volatile static boolean useKeyCoder = true;

    public enum Mode {
        STRICT, LOOSE, DISABLED
    }

    static {
        try {
            final String resourceName = System.getProperty(COLLATION_PROPERTIES_FILE_NAME_PROPERTY,
                    DEFAULT_PROPERTIES_FILE_NAME);
            collationNameProperties.clear();
            collationNameProperties.load(AkCollatorFactory.class.getResourceAsStream(resourceName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Set factory to one of three modes specified by case-insensitive string:
     * <dl>
     * <dt>disabled</dt>
     * <dd>always uses UCS_BINARY_COLLATOR</dd>
     * <dt>strict</dt>
     * <dd>disallows unimplemented collators</dd>
     * <dt>loose</dt>
     * <dd>returns UCS_BINARY_COLLATOR for any unrecognized name</dd>
     * </dl
     * 
     * @param modeString
     */
    public static void setCollationMode(String modeString) {
        try {
            setCollationMode(Mode.valueOf(modeString.toUpperCase()));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Collation mode must be STRICT, LOOSE or DISABLED: " + modeString);
        }
    }

    public static void setCollationMode(Mode m) {
        if (m == null) {
            throw new NullPointerException();
        }
        collationIdMap.clear();
        collatorMap.clear();
        mode = m;
    }

    public static Mode getCollationMode() {
        return mode;
    }

    /**
     * @param name
     * @return an AkCollator
     */
    public static AkCollator getAkCollator(final String name) {
        if (mode == Mode.DISABLED || name == null) {
            return UCS_BINARY_COLLATOR;
        }
        SoftReference<AkCollator> ref = collatorMap.get(name);
        if (ref != null) {
            AkCollator akCollator = ref.get();
            if (akCollator != null) {
                cacheHits++;
                return akCollator;
            }
        }

        final String idAndScheme = schemeForName(name);
        if (idAndScheme == null) {
            if (mode == Mode.LOOSE) {
                return mapToBinary(name);
            } else {
                throw new UnsupportedCollationException(name);
            }
        }

        final Matcher matcher = SCHEMA_PATTERN.matcher(idAndScheme);
        if (!matcher.matches()) {
            throw new IllegalStateException("collation name " + name + " has malformed value " + idAndScheme);
        }
        final String scheme = matcher.group(2);
        if (scheme.startsWith(UCS_BINARY)) {
            return mapToBinary(name);
        }

        synchronized (collatorMap) {
            final int collationId;
            try {
                collationId = Integer.parseInt(matcher.group(1));
                if (collationId < 0 || collationId > MAX_COLLATION_ID) {
                    throw new IllegalStateException("collation name " + name + " has invalid ID " + collationId);
                }
            } catch (Exception e) {
                throw new IllegalStateException("collation name " + name + " has malformed value " + idAndScheme);
            }

            ref = collationIdMap.get(collationId);
            if (ref == null || ref.get() == null) {
                final AkCollator akCollator;
                if (scheme.startsWith(MYSQL)) {
                    akCollator = new AkCollatorMySQL(name, scheme, collationId,
                            collationNameProperties.getProperty(scheme), useKeyCoder);
                } else {
                    akCollator = new AkCollatorICU(name, scheme, collationId, useKeyCoder);
                }

                ref = new SoftReference<>(akCollator);
                collatorMap.put(name, ref);
                if (!collationIdMap.containsKey(collationId)) {
                    collationIdMap.put(collationId, ref);
                }
                return akCollator;

            } else {
                collatorMap.put(name, ref);
                return ref.get();
            }
        }
    }
    
    public static AkCollator getAkCollator(final int collatorId) {
        final SoftReference<AkCollator> ref = collationIdMap.get(collatorId);
        AkCollator collator = (ref == null ? null : ref.get());
        if (collator == null) {
            if (collatorId == 0) {
                return UCS_BINARY_COLLATOR;
            }
            for (Map.Entry<Object, Object> entry : collationNameProperties.entrySet()) {
                final Matcher matcher = SCHEMA_PATTERN.matcher(entry.getValue().toString());
                if (matcher.matches() && matcher.group(1).equals(Integer.toString(collatorId))) {
                    return getAkCollator(entry.getKey().toString());
                }
            }
        } else {
            cacheHits++;
        }
        return collator;
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
     * com.foundationdb.server.collation.collation_names.properties
     * </code>
     * </pre>
     * 
     * This location can be overridden with the system property named
     * 
     * <pre>
     * <code>
     * foundationdb.collation.properties
     * </code>
     * </pre>
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
                throw new IllegalStateException("Malformed property for name " + scheme + ": " + e);
            }

            collator = Collator.getInstance(new ULocale(locale));
            collator.setStrength(strength);
            sourceMap.put(scheme, collator);
        }
        collator = collator.cloneAsThawed();
        return collator;
    }

    private static AkCollator mapToBinary(final String name) {
        collatorMap.put(name, new SoftReference<>(UCS_BINARY_COLLATOR));
        return UCS_BINARY_COLLATOR;
    }

    private synchronized static String schemeForName(final String name) {
        final String lcname = name.toLowerCase();
        String scheme = collationNameProperties.getProperty(lcname);
        return scheme;
    }

    /**
     * Intended only for unit tests.
     * 
     * @return Number of times either getAkCollator() method has returned a
     *         cached value.
     */
    static int getCacheHits() {
        return cacheHits;
    }

    public static boolean isUseKeyCoder() {
        return useKeyCoder;
    }

    public static void setUseKeyCoder(boolean x) {
        useKeyCoder = x;
    }
}
