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

package com.akiban.server.mttests.mthapi.base;

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
