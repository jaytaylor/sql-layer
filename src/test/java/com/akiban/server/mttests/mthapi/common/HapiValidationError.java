package com.akiban.server.mttests.mthapi.common;

import org.json.JSONObject;
import org.junit.Assert;

import static org.junit.Assert.*;

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

    static class ResponseIsNullError extends HapiValidationError{ private ResponseIsNullError(AssertionError cause) {super(cause);}}
    static class RootElementsCountError extends HapiValidationError{ private RootElementsCountError(AssertionError cause) {super(cause);}}
    static class RootTablesCountError extends HapiValidationError{ private RootTablesCountError(AssertionError cause) { super(cause);}}
    static class RootTableNameError extends HapiValidationError{ private RootTableNameError(AssertionError cause) {super(cause);}}
    static class UnseenPredicatesError extends HapiValidationError{ private UnseenPredicatesError(AssertionError cause) {super(cause);}}
    static class FieldsMissingError extends HapiValidationError{ private FieldsMissingError(AssertionError cause) {super(cause);}}
    static class InvalidFieldError extends HapiValidationError{ private InvalidFieldError(AssertionError cause) {super(cause);}}
    static class FkMismatchError extends HapiValidationError{ private FkMismatchError(AssertionError cause) {super(cause);}}

    HapiValidationError(AssertionError cause) {
        super(cause.getMessage());
    }

    static Void launder(AssertionError e, Reason reason) {
        // return type of Void (not void) checks for us that an assertion is thrown through all code paths
        if (reason != null) {
            switch (reason) {
                case RESPONSE_IS_NULL:
                    throw new ResponseIsNullError(e);
                case ROOT_TABLES_COUNT:
                    throw new RootTablesCountError(e);
                case ROOT_ELEMENTS_COUNT:
                    throw new RootElementsCountError(e);
                case ROOT_TABLE_NAME:
                    throw new RootTableNameError(e);
                case UNSEEN_PREDICATES:
                    throw new UnseenPredicatesError(e);
                case FIELDS_MISSING:
                    throw new FieldsMissingError(e);
                case INVALID_FIELD:
                    throw new InvalidFieldError(e);
                case FK_MISMATCH:
                    throw new FkMismatchError(e);
            }
        }
        throw e;
    }

    public static void hapiassertNotNull(Reason reason, JSONObject response) {
        try {
            assertNotNull("response is null", response);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }

    public static void assertEquals(Reason reason, String message, int expected, int actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }

    public static <T> void assertEquals(Reason reason, String message, T expected, T actual) {
        try {
            Assert.assertEquals(message, expected, actual);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }

    public static void assertTrue(Reason reason, String message, boolean condition) {
        try {
            Assert.assertTrue(message, condition);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }

    public static void assertFalse(Reason reason, String message, boolean condition) {
        try {
            Assert.assertFalse(message, condition);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }

    public static void fail(Reason reason, String message) {
        try {
            Assert.fail(message);
        } catch (AssertionError e) {
            launder(e, reason);
        }
    }
}
