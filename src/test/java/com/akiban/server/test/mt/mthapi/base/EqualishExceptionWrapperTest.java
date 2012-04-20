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

package com.akiban.server.test.mt.mthapi.base;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

public final class EqualishExceptionWrapperTest {

    @Test
    public void testEquality() {
        EqualishExceptionWrapper hello = generate(3, new ExceptionGenerator("hello"));
        EqualishExceptionWrapper world = generate(3, new ExceptionGenerator("world"));

        assertTrue("by equality", hello.equals(world));
        assertTrue("hash codes", hello.hashCode() == world.hashCode());
    }

    @Test
    public void differentStack() {
        EqualishExceptionWrapper hello = generate(3, new ExceptionGenerator("hello"));
        EqualishExceptionWrapper world = generate(5, new ExceptionGenerator("world"));

        assertFalse("by equality", hello.equals(world));
        assertFalse("hash codes", hello.hashCode() == world.hashCode());
    }

    @Test
    public void differentCause() {
        EqualishExceptionWrapper hello = generate(3, new ExceptionGenerator("hello"));
        EqualishExceptionWrapper world = generate(3, new ExceptionGenerator("world") {
            @Override
            protected void generate(String message) throws Exception {
                throw new IOException(message);
            }
        });

        assertFalse("by equality", hello.equals(world));
        assertFalse("hash codes", hello.hashCode() == world.hashCode());
    }

    private static class ExceptionGenerator {
        private final String message;

        private ExceptionGenerator(String message) {
            this.message = message;
        }

        public final void generate() throws Exception {
            generate(message);
        }

        protected void generate(String message) throws Exception {
            throw new Exception(message);
        }
    }

    private static EqualishExceptionWrapper generate(final int stackFrames, final ExceptionGenerator generator) {
        final AtomicReference<EqualishExceptionWrapper> atomic = new AtomicReference<EqualishExceptionWrapper>();
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    countTillException(stackFrames, generator);
                } catch (Throwable e) {
                    atomic.set(new EqualishExceptionWrapper(e));
                }
            }
        };
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return atomic.get();
    }

    private static void countTillException(int stackFrames, ExceptionGenerator generator) throws Exception {
        if (stackFrames <= 0) {
            generator.generate();
        }
        countTillException(stackFrames - 1, generator);
    }
}
