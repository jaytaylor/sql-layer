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

package com.foundationdb.util;

import com.google.common.base.Functions;
import com.google.common.collect.Lists;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
            collection.add(null);
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

    public static void expectMultipleCause(Runnable runnable, Class... expected) {
        List<Class> expectedList = Arrays.asList(expected);
        try {
            runnable.run();
            fail("expected exception");
        } catch(MultipleCauseException e) {
            for(Throwable c : e.getCauses()) {
                if(!expectedList.contains(c.getClass())) {
                    fail("Unexpected cause: " + c);
                }
                assertEquals("Total causes", expected.length, e.getCauses().size());
            }
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
