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

/**
 * Alpha <- Beta <- Gamma <- nothing
 */
final class DummyErroringServices {

    public static class ErroringAlpha implements DummyInterfaces.Alpha {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Beta.class, GuicerTest.MESSAGING_ACTIONS)) {
                throw new Error("how can this instance be equal to an instance of another class?!");
            }
        }

        @Override
        public void stop() {}
    }

    public static class ErroringBeta implements DummyInterfaces.Beta {
        @Override
        public void start() {
            if (this == GuicerTest.onlyGuicer().get(DummyInterfaces.Gamma.class, GuicerTest.MESSAGING_ACTIONS)) {
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
}
