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

package com.akiban.server.mttests.mthapi.common;

import org.json.JSONObject;
import org.junit.Assert;

public class HapiValidationError extends AssertionError {
    enum Reason {
        RESPONSE_IS_NULL,
        ROOT_TABLES_COUNT,
        ROOT_ELEMENTS_COUNT,
        ROOT_TABLE_NAME,
        UNSEEN_PREDICATES,
        FIELDS_MISSING,
        INVALID_FIELD,
        FK_MISMATCH,
    }

    static class ResponseIsNullError extends HapiValidationError{ private ResponseIsNullError(String message) {super(message);}}
    static class RootElementsCountError extends HapiValidationError{ private RootElementsCountError(String message) {super(message);}}
    static class RootTablesCountError extends HapiValidationError{ private RootTablesCountError(String message) { super(message);}}
    static class RootTableNameError extends HapiValidationError{ private RootTableNameError(String message) {super(message);}}
    static class UnseenPredicatesError extends HapiValidationError{ private UnseenPredicatesError(String message) {super(message);}}
    static class FieldsMissingError extends HapiValidationError{ private FieldsMissingError(String message) {super(message);}}
    static class InvalidFieldError extends HapiValidationError{ private InvalidFieldError(String message) {super(message);}}
    static class FkMismatchError extends HapiValidationError{ private FkMismatchError(String message) {super(message);}}

    HapiValidationError(String message) {
        super(message);
    }

    static Void launder(String message, Reason reason) {
        // return type of Void (not void) checks for us that an assertion is thrown through all code paths
        if (reason != null) {
            switch (reason) {
                case RESPONSE_IS_NULL:
                    throw new ResponseIsNullError(message);
                case ROOT_TABLES_COUNT:
                    throw new RootTablesCountError(message);
                case ROOT_ELEMENTS_COUNT:
                    throw new RootElementsCountError(message);
                case ROOT_TABLE_NAME:
                    throw new RootTableNameError(message);
                case UNSEEN_PREDICATES:
                    throw new UnseenPredicatesError(message);
                case FIELDS_MISSING:
                    throw new FieldsMissingError(message);
                case INVALID_FIELD:
                    throw new InvalidFieldError(message);
                case FK_MISMATCH:
                    throw new FkMismatchError(message);
            }
        }
        throw new AssertionError(message);
    }

    public static void assertNotNull(Reason reason, JSONObject response) {
        assertTrue(reason, "response is null", response != null);
    }

    public static void assertEquals(Reason reason, String message, int expected, int actual) {
        if (expected != actual) {
            launder(notEqualsMessage(message, expected, actual), reason);
        }
    }

    public static <T> void assertEquals(Reason reason, String message, T expected, T actual) {
        if (expected == null) {
            if (actual != null) {
                launder(notEqualsMessage(message, expected, actual), reason);
            }
        }
        else if (!expected.equals(actual)) {
            launder(notEqualsMessage(message, expected, actual), reason);
        }
    }

    public static void assertTrue(Reason reason, String message, boolean condition) {
        if (!condition) {
            launder(message, reason);
        }
    }

    public static void assertFalse(Reason reason, String message, boolean condition) {
        if (condition) {
            launder(message, reason);
        }
    }

    public static void fail(Reason reason, String message) {
        launder(message, reason);
    }

    private static <T> String notEqualsMessage(String message, T expected, T actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (AssertionError e) {
            return e.getMessage();
        }
        throw new AssertionError(String.format(
                "HapiValidationError: thought these were unequal, but they're not!: <%s> and <%s>",
                expected, actual)
        );
    }
}
