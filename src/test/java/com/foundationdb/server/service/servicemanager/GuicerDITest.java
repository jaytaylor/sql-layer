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

package com.foundationdb.server.service.servicemanager;

import org.junit.Test;

import com.foundationdb.server.error.CircularDependencyException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.fail;

/**
 * <p>Tests the startup of various services. Here's the dependency graph:
 * <pre>
 *        A   <---+
 *      ↙   ↘     ¦
 *     B     C    ¦
 *  ↙  ↓     ↓    ¦
 * E   D     ↓    ¦
 *  ↖   ↘   ↙     ¦
 *    ← ← F - - - +
 * </pre>
 * </p>
 * <p>The dotted line from F to A represents a circular dependency. We don't allow this, so most tests use an
 * implementation of F that does <em>not</em> depend on A. The one that does, we're looking for an exception.</p>
 */
public final class GuicerDITest {

    @Test
    public void nonCircularFromA() {
        standardTester()
                .startAndStop(A.class)
                .check(AImpl.class, BImpl.class, CImpl.class, EImpl.class, DImpl.class, FImpl.class)
                .checkDependencies(AImpl.class, BImpl.class, DImpl.class, CImpl.class, FImpl.class, EImpl.class)
                .checkDependencies(BImpl.class, DImpl.class, FImpl.class, EImpl.class)
                .checkDependencies(CImpl.class, FImpl.class, EImpl.class)
                .checkDependencies(DImpl.class, FImpl.class, EImpl.class)
                .checkDependencies(EImpl.class)
                .checkDependencies(FImpl.class, EImpl.class)
        ;
    }
    
    @Test
    public void nonCircularFromF() {
        standardTester()
                .startAndStop(F.class)
                .check(EImpl.class, FImpl.class)
                .checkDependencies(EImpl.class)
                .checkDependencies(FImpl.class, EImpl.class)
        ;
    }


    @Test
    public void nonCircularFromE() {
        standardTester()
                .startAndStop(E.class)
                .check(EImpl.class)
        ;
    }

    @Test(expected = CircularDependencyException.class)
    public void circular() {
        GuiceInjectionTester tester = standardTester().bind(F.class, CircularF.class).check();
        try {
            tester.startAndStop(F.class);
            fail("expected a CircularDependencyException");
        } catch (CircularDependencyException e) {
            tester.check();
            throw e;
        }
    }
    
    @Test
    public void fieldInjection() {
        new GuiceInjectionTester()
                .bind(FieldInjectionA.class, FieldInjectionAImpl.class)
                .bind(FieldInjectionB.class, FieldInjectionBImpl.class)
                .startAndStop(FieldInjectionA.class)
                .check(FieldInjectionAImpl.class, FieldInjectionBImpl.class)
                .checkDependencies(FieldInjectionAImpl.class, FieldInjectionBImpl.class);
    }

    @Test
    public void methodInjection() {
        new GuiceInjectionTester()
                .bind(MethodInjectionA.class, MethodInjectionAImpl.class)
                .bind(MethodInjectionB.class, MethodInjectionBImpl.class)
                .startAndStop(MethodInjectionA.class)
                .check(MethodInjectionAImpl.class, MethodInjectionBImpl.class)
                .checkDependencies(MethodInjectionAImpl.class, MethodInjectionBImpl.class);
    }

    private static GuiceInjectionTester standardTester() {
        return new GuiceInjectionTester()
                .bind(A.class, AImpl.class)
                .bind(B.class, BImpl.class)
                .bind(C.class, CImpl.class)
                .bind(D.class, DImpl.class)
                .bind(E.class, EImpl.class)
                .bind(F.class, FImpl.class);
    }

    public static abstract class Interesting {
        
        protected Interesting(Object... dependencies) {
            this.dependencies = Arrays.asList(dependencies);
        }

        @Override
        public String toString() {
            List<String> dependencyClasses = new ArrayList<>();
            for (Object dependency : dependencies) {
                dependencyClasses.add(dependency.getClass().getSimpleName());
            }
            return getClass().getSimpleName() + " directly depends on " + dependencyClasses;
        }

        private List<?> dependencies;
    }

    public interface A {}

    public interface B {}

    public interface C {}

    public interface D {}

    public interface E {}

    public interface F {}

    public static class AImpl extends Interesting implements A {
        @Inject
        public AImpl(B b, C c) {
            super(b, c);
        }
    }

    public static class BImpl extends Interesting implements B {
        @Inject
        public BImpl(D d, E e) {
            super(d, e);
        }
    }

    public static class CImpl extends Interesting implements C {
        @Inject
        public CImpl(F f) {
            super(f);
        }
    }
    
    public static class DImpl extends Interesting implements D {
        @Inject
        public DImpl(F f) {
            super(f);
        }
    }

    public static class EImpl extends Interesting implements E {
    }

    public static class FImpl extends Interesting implements F {
        @Inject
        public FImpl(E e) {
            super(e);
        }
    }

    public static class CircularF extends Interesting implements F {
        @Inject
        public CircularF(A a) {
            super(a);
        }
    }

    public interface FieldInjectionA {}
    public interface FieldInjectionB {}
    
    public static class FieldInjectionAImpl implements FieldInjectionA {
        @SuppressWarnings("unused") @Inject private FieldInjectionB b;
    }
    
    public static class FieldInjectionBImpl implements FieldInjectionB {}

    public interface MethodInjectionA {}
    public interface MethodInjectionB {}

    public static class MethodInjectionAImpl implements MethodInjectionA {
        @SuppressWarnings("unused") @Inject
        private void setDependency(MethodInjectionB b) {
            assert b != null;
        }
    }

    public static class MethodInjectionBImpl implements MethodInjectionB {}

    @Test
    public void managerInjection() {
        M m = new MImpl(1.0);
        new GuiceInjectionTester()
                .manager(M.class, m)
                .bind(N.class, NImpl.class)
                .startAndStop(N.class);
        assert (m.wasSeen());
    }

    public interface M extends ServiceManagerBase {
        boolean wasSeen();
        void seen();
    }

    public interface N {
    }

    public static class MImpl implements M {
        // No no-arg ctor.
        public MImpl(double d) {
        }

        private boolean seen = false;
        @Override
        public boolean wasSeen() {
            return seen;
        }
        @Override
        public void seen() {
            seen = true;
        }
    }

    public static class NImpl implements N {
        @Inject
        public NImpl(M mgr) {
            mgr.seen();
        }
    }

}
