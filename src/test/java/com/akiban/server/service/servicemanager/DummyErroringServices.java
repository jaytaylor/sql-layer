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

package com.akiban.server.service.servicemanager;

/**
 * Alpha <- Beta <- Gamma <- nothing
 */
public final class DummyErroringServices {

    public static class ErroringAlpha implements DummyInterfaces.Alpha {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Beta.class)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
        }

        @Override
        public void stop() {}
    }

    public static class ErroringBeta implements DummyInterfaces.Beta {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Gamma.class)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
            throw new ErroringException();
        }

        @Override
        public void stop() {}
    }

    public static class ErroringGamma implements DummyInterfaces.Gamma {
        @Override
        public void start() {}

        @Override
        public void stop() {}
    }

    static class ErroringException extends RuntimeException {}

    // private methods

    private static void sayStarting(Object instance) {
        DummyInterfaces.addMessage(instance.getClass().getSimpleName() + " starting");
    }

    private static void sayStopping(Object instance) {
        DummyInterfaces.addMessage(instance.getClass().getSimpleName() + " stopping");
    }
}
