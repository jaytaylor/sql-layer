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

package com.akiban.server.t3expressions;

import com.akiban.server.types3.T3TestClass;
import com.akiban.server.types3.TCast;
import com.akiban.server.types3.TCastBase;
import com.akiban.server.types3.TCastPath;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.service.InstanceFinder;
import com.akiban.server.types3.texpressions.Constantness;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.junit.Test;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public final class T3RegistryImplTest {
    private final TClass CLASS_A = new T3TestClass("A");
    private final TClass CLASS_B = new T3TestClass("B");
    private final TClass CLASS_C = new T3TestClass("C");
    private final TClass CLASS_D = new T3TestClass("D");
    private final TClass CLASS_E = new T3TestClass("E");

    @Test
    public void createSelfCasts() {
        InstanceFinder finder = new Finder();
        Map<TClass, Map<TClass, TCast>> casts = T3RegistryServiceImpl.createCasts(finder.find(TClass.class), finder);
        checkAll(casts,
                new SelfCastCheck(CLASS_A, CLASS_A),
                new SelfCastCheck(CLASS_B, CLASS_B),
                new SelfCastCheck(CLASS_C, CLASS_C),
                new SelfCastCheck(CLASS_D, CLASS_D),
                new SelfCastCheck(CLASS_E, CLASS_E));
    }

    @Test
    public void simpleCastPath() {
        Finder finder = new Finder();
        TCastPath path = TCastPath.create(CLASS_A, CLASS_B, CLASS_C);
        finder.put(TCastPath.class, path);
        Map<TClass, Map<TClass, TCast>> casts = T3RegistryServiceImpl.createCasts(finder.find(TClass.class), finder);
        singleStepCasts(casts);
        T3RegistryServiceImpl.createDerivedCasts(casts, finder);
        checkAll(casts,
                // self casts
                new SelfCastCheck(CLASS_A, CLASS_A),
                new SelfCastCheck(CLASS_B, CLASS_B),
                new SelfCastCheck(CLASS_C, CLASS_C),
                new SelfCastCheck(CLASS_D, CLASS_D),
                new SelfCastCheck(CLASS_E, CLASS_E),
                // single-step casts
                new SelfCastCheck(CLASS_A, CLASS_B),
                new SelfCastCheck(CLASS_B, CLASS_C),
                new SelfCastCheck(CLASS_C, CLASS_D),
                new SelfCastCheck(CLASS_D, CLASS_E),
                // cast path
                new SelfCastCheck(CLASS_A, CLASS_C)
        );
    }

    @Test
    public void jumpingCastPath() {
        Finder finder = new Finder();
        finder.put(TCastPath.class, TCastPath.create(CLASS_A, CLASS_B, CLASS_C, CLASS_D, CLASS_E));

        Map<TClass, Map<TClass, TCast>> casts = T3RegistryServiceImpl.createCasts(finder.find(TClass.class), finder);
        singleStepCasts(casts);
        putCast(new BogusCast(CLASS_A, CLASS_D), casts);
        T3RegistryServiceImpl.createDerivedCasts(casts, finder);
        checkAll(casts,
                // self casts
                new SelfCastCheck(CLASS_A, CLASS_A),
                new SelfCastCheck(CLASS_B, CLASS_B),
                new SelfCastCheck(CLASS_C, CLASS_C),
                new SelfCastCheck(CLASS_D, CLASS_D),
                new SelfCastCheck(CLASS_E, CLASS_E),
                // cast path from A
                new SelfCastCheck(CLASS_A, CLASS_B),
                new SelfCastCheck(CLASS_A, CLASS_C),
                new SelfCastCheck(CLASS_A, CLASS_D),
                new SelfCastCheck(CLASS_A, CLASS_E),
                // cast path from B
                new SelfCastCheck(CLASS_B, CLASS_C),
                new SelfCastCheck(CLASS_B, CLASS_D),
                new SelfCastCheck(CLASS_B, CLASS_E),
                // cast path from C
                new SelfCastCheck(CLASS_C, CLASS_D),
                new SelfCastCheck(CLASS_C, CLASS_E),
                // cast path from D
                new SelfCastCheck(CLASS_D, CLASS_E)
        );
    }

    private void singleStepCasts(Map<TClass, Map<TClass, TCast>> casts) {
        putCast(new BogusCast(CLASS_A, CLASS_B), casts);
        putCast(new BogusCast(CLASS_B, CLASS_C), casts);
        putCast(new BogusCast(CLASS_C, CLASS_D), casts);
        putCast(new BogusCast(CLASS_D, CLASS_E), casts);
    }

    private void putCast(TCast cast, Map<TClass, Map<TClass, TCast>> outMap) {
        Object o = outMap.get(cast.sourceClass()).put(cast.targetClass(), cast);
        assert o == null : "putting " + cast + " into " + outMap; // shouldn't happen
    }

    private class Finder implements InstanceFinder {
        @Override
        @SuppressWarnings("unchecked")
        public <T> Collection<? extends T> find(Class<? extends T> targetClass) {
            Collection<?> resultWild = instances.get(targetClass);
            return (Collection<? extends T>) resultWild;
        }

        public void put(Class<?> cls, Object o) {
            instances.put(cls, o);
        }

        Finder() {
            put(TClass.class, CLASS_A);
            put(TClass.class, CLASS_B);
            put(TClass.class, CLASS_C);
            put(TClass.class, CLASS_D);
            put(TClass.class, CLASS_E);
        }

        private Multimap<Class<?>,Object> instances = ArrayListMultimap.create();
    }

    private void checkAll(Map<TClass, Map<TClass, TCast>> actual, SelfCastCheck... expected) {
        // translate the SelfCastCheck[] to a Map
        Map<TClass,Map<TClass,SelfCastCheck>> expectedMap = tClassMap();
        for (SelfCastCheck check : expected) {
            Map<TClass, SelfCastCheck> map = expectedMap.get(check.source);
            if (map == null) {
                map = tClassMap();
                expectedMap.put(check.source, map);
            }
            SelfCastCheck old = map.put(check.target, check);
            assertNull("duplicate for " + check, old);
        }

        // Translate the casts map to use SelfCastCheck
        Map<TClass,Map<TClass,SelfCastCheck>> actualsMap = tClassMap();
        for (Map.Entry<TClass,Map<TClass,TCast>> bySourceEntry : actual.entrySet()) {
            TClass source = bySourceEntry.getKey();
            Map<TClass, SelfCastCheck> translated = tClassMap();
            Object old = actualsMap.put(source, translated);
            assert old == null : actual; // shouldn't happen!
            for (Map.Entry<TClass,TCast> byTargetEntry : bySourceEntry.getValue().entrySet()) {
                TClass target = byTargetEntry.getKey();
                TCast cast = byTargetEntry.getValue();
                SelfCastCheck check = new SelfCastCheck(cast.sourceClass(), cast.targetClass());
                old = translated.put(target, check);
                assertNull("duplicate with " + bySourceEntry, old);
            }
        }

        // now just check the two maps
        assertEquals("casts map", toString(expectedMap), toString(actualsMap)); // easy-to-view diff in IDEs
        assertEquals("casts map", expectedMap, actualsMap);
    }

    private String toString(Map<TClass, Map<TClass, SelfCastCheck>> map) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<TClass, Map<TClass, SelfCastCheck>> entry : map.entrySet()) {
            sb.append(entry.getKey()).append('\n');
            for (Map.Entry<TClass,SelfCastCheck> subentry: entry.getValue().entrySet()) {
                sb.append("    ").append(subentry).append('\n');
            }
        }
        return sb.toString();
    }

    private <V> TreeMap<TClass, V> tClassMap() {
        return new TreeMap<TClass, V>(tClassComparator);
    }

    private static class SelfCastCheck {

        @Override
        public String toString() {
            return "cast(" + source + " to " + target + ")";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SelfCastCheck that = (SelfCastCheck) o;

            return source.equals(that.source) && target.equals(that.target);

        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + target.hashCode();
            return result;
        }

        private SelfCastCheck(TClass source, TClass target) {
            this.source = source;
            this.target = target;
        }

        private final TClass source;
        private final TClass target;
    }

    private static class BogusCast extends TCastBase {
        private BogusCast(TClass sourceClass, TClass targetClass) {
            super(sourceClass, targetClass, false, Constantness.UNKNOWN);
        }

        @Override
        public void evaluate(TExecutionContext context, PValueSource source, PValueTarget target) {
            throw new UnsupportedOperationException();
        }
    }

    private Comparator<TClass> tClassComparator = new Comparator<TClass>() {
        @Override
        public int compare(TClass o1, TClass o2) {
            return o1.toString().compareTo(o2.toString());
        }
    };
}
