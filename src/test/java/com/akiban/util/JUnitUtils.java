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

package com.akiban.util;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class JUnitUtils {

    public static void equalMaps(String message, Map<?, ?> expected, Map<?, ?> actual) {
        List<String> expectedStrings = Strings.entriesToString(expected);
        List<String> actualStrings = Strings.entriesToString(actual);
        Collections.sort(expectedStrings);
        Collections.sort(actualStrings);
        equalCollections(message, expectedStrings, actualStrings);
    }

    public static void equalCollections(String message, Collection<?> expected, Collection<?> actual) {
        if (expected == null) {
            assertEquals(message, expected, actual);
        }
        else if (!expected.equals(actual)) {
            assertEquals(message, Strings.join(expected), Strings.join(actual));
            assertEquals(message, expected, actual);
        }
    }

    public static void equalsIncludingHash(String message, Object expected, Object actual) {
        assertEquals(message, expected, actual);
        assertEquals(message + " (hash code", expected.hashCode(), actual.hashCode());
    }

    public static <T> void isUnmodifiable(String message, Collection<T> collection) {
        try {
            List<T> copy = new ArrayList<>(collection);
            collection.clear(); // good enough proxy for all modifications, for the JDK classes anyway
            collection.addAll(copy); // restore the contents, in case someone wants to look in a debugger
            fail("collection is modifable: " + message);
        } catch (UnsupportedOperationException e) {
            // swallow
        }
    }

    public static <K, V> void isUnmodifiable(String message, Map<K, V> map) {
        try {
            Map<K, V> copy = new HashMap<>(map);
            map.clear(); // good enough proxy for all modifications, for the JDK classes anyway
            map.putAll(copy); // restore the map's contents, in case someone wants to look in a debugger
            fail("map is modifable: " + message);
        } catch (UnsupportedOperationException e) {
            // swallow
        }
    }

    public static <K, V> BuildingMap<K, V> map(K key, V value) {
        BuildingMap<K, V> map = new BuildingMap<>();
        map.put(key, value);
        return map;
    }

    public static File getContainingFile(Class<?> cls) {
        String path = "src/test/resources/" + cls.getCanonicalName().replace('.', File.separatorChar);
        return new File(path).getParentFile();
    }

    public static String normalizeJson(String jsonString) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(new StringReader(jsonString));
            return mapper.defaultPrettyPrintingWriter().writeValueAsString(node);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static class BuildingMap<K,V> extends HashMap<K,V> {
        public BuildingMap<K, V> and(K key, V value) {
            put(key, value);
            return this;
        }

        private BuildingMap() {}
    }

    public static abstract class MessageTaker {

        private final List<String> messages = new ArrayList<>();

        protected final void message(String label) {
            messages.add(label);
        }

        protected final void message(String label, Object... args) {
            List<String> line = Lists.transform(asList(args), Functions.toStringFunction());
            messages.add(label +": " + line);
        }

        public final List<String> getMessages() {
            return messages;
        }
    }

    private JUnitUtils() {}
}
