/**
 * Copyright (C) 2011 Akiban Technologies Inc.
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses.
 */

package com.akiban.util;

public final class Exceptions {
    /**
     * Throws the given throwable, downcast, if it's of the appropriate type
     *
     * @param t the exception to check
     * @param cls  the class to check for and cast to
     * @throws T the e instance, cast down
     */
    public static <T extends Throwable> void throwIfInstanceOf(Throwable t, Class<T> cls) throws T {
        if (cls.isInstance(t)) {
            throw cls.cast(t);
        }
    }

    /**
     * <p>Always throws something. If {@code t} is a RuntimeException, it simply gets thrown. If it's a checked
     * exception, it gets wrapped in a RuntimeException. If it's an Error, it simply gets thrown. And if somehow
     * it's something else, that thing is wrapped in an Error and thrown.</p>
     *
     * <p>The return value of Error is simply as a convenience for methods that return a non-void type; you can
     * invoke {@code throw throwAlways(t);} to indicate to the compiler that the method's control will end there.</p>
     * @param t the throwable to throw, possibly wrapped if needed
     * @return nothing, since something is always thrown from this method.
     */
    public static Error throwAlways(Throwable t) {
        throwIfInstanceOf(t,RuntimeException.class);
        if (t instanceof Exception) {
            throw new RuntimeException(t);
        }
        throwIfInstanceOf(t, Error.class);
        throw new Error("not a RuntimeException, checked exception or Error?!", t);
    }

    /*
    Python script for creating the following methods:
    
    def generate(n):
        out = ""
        out += '@SuppressWarnings("unused") public static\n<'
        out += ', '.join(['E%d extends Throwable' % i for i in range(n)])
        out += ">\nvoid throwIfInstanceOf(Throwable t, "
        out += ', '.join(['Class<E%d> e%d' % (i, i) for i in range(n)])
        out += ")\nthrows %s {\n" % ', '.join(['E%d' % i for i in range(n)])
        out += "\n".join(["throwIfInstanceOf(t, e%d);" % i for i in range(n)])
        out += "\n}\n"
        print out
     */

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1)
            throws E0, E1 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2)
            throws E0, E1, E2 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3)
            throws E0, E1, E2, E3 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4)
            throws E0, E1, E2, E3, E4 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5)
            throws E0, E1, E2, E3, E4, E5 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6)
            throws E0, E1, E2, E3, E4, E5, E6 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7)
            throws E0, E1, E2, E3, E4, E5, E6, E7 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable, E8 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7, Class<E8> e8)
            throws E0, E1, E2, E3, E4, E5, E6, E7, E8 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
        throwIfInstanceOf(t, e8);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable, E8 extends Throwable, E9 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7, Class<E8> e8, Class<E9> e9)
            throws E0, E1, E2, E3, E4, E5, E6, E7, E8, E9 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
        throwIfInstanceOf(t, e8);
        throwIfInstanceOf(t, e9);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable, E8 extends Throwable, E9 extends Throwable, E10 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7, Class<E8> e8, Class<E9> e9, Class<E10> e10)
            throws E0, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
        throwIfInstanceOf(t, e8);
        throwIfInstanceOf(t, e9);
        throwIfInstanceOf(t, e10);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable, E8 extends Throwable, E9 extends Throwable, E10 extends Throwable, E11 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7, Class<E8> e8, Class<E9> e9, Class<E10> e10, Class<E11> e11)
            throws E0, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
        throwIfInstanceOf(t, e8);
        throwIfInstanceOf(t, e9);
        throwIfInstanceOf(t, e10);
        throwIfInstanceOf(t, e11);
    }

    @SuppressWarnings("unused")
    public static <E0 extends Throwable, E1 extends Throwable, E2 extends Throwable, E3 extends Throwable, E4 extends Throwable, E5 extends Throwable, E6 extends Throwable, E7 extends Throwable, E8 extends Throwable, E9 extends Throwable, E10 extends Throwable, E11 extends Throwable, E12 extends Throwable>
    void throwIfInstanceOf(Throwable t, Class<E0> e0, Class<E1> e1, Class<E2> e2, Class<E3> e3, Class<E4> e4, Class<E5> e5, Class<E6> e6, Class<E7> e7, Class<E8> e8, Class<E9> e9, Class<E10> e10, Class<E11> e11, Class<E12
            > e12)
            throws E0, E1, E2, E3, E4, E5, E6, E7, E8, E9, E10, E11, E12 {
        throwIfInstanceOf(t, e0);
        throwIfInstanceOf(t, e1);
        throwIfInstanceOf(t, e2);
        throwIfInstanceOf(t, e3);
        throwIfInstanceOf(t, e4);
        throwIfInstanceOf(t, e5);
        throwIfInstanceOf(t, e6);
        throwIfInstanceOf(t, e7);
        throwIfInstanceOf(t, e8);
        throwIfInstanceOf(t, e9);
        throwIfInstanceOf(t, e10);
        throwIfInstanceOf(t, e11);
        throwIfInstanceOf(t, e12);
    }

}
