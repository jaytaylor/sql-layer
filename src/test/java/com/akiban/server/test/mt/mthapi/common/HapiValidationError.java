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

package com.akiban.server.test.mt.mthapi.common;

import com.akiban.server.api.HapiGetRequest;
import org.json.JSONObject;
import org.junit.Assert;

public class HapiValidationError extends AssertionError {

    private volatile JSONObject jsonObject;
    private volatile HapiGetRequest getRequest;

    public enum Reason {
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

    public void setJsonObject(JSONObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public void setGetRequest(HapiGetRequest request) {
        this.getRequest = request;
    }

    @Override
    public String toString() {
        return String.format("%s: %s => %s", super.toString(), getRequest, jsonObject);
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
