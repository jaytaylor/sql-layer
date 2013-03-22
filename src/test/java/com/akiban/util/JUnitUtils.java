
package com.akiban.util;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;

import java.io.File;
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
