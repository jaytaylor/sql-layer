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

package com.akiban.server.types.typestests;

import com.akiban.server.types.AkType;
import com.akiban.server.types.WrongValueGetException;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.fail;

public final class ConversionSuite<T> {

    public static <T> SuiteBuilder<T> build(LinkedConversion<? super T> converters) {
        return new SuiteBuilder<T>(converters);
    }

    public TestCase<?> testCaseAt(int index) {
        return testCases.get(index);
    }

    public ConversionSuite(LinkedConversion<? super T> converters, List<TestCase<? extends T>> testCases) {
        this.testCases = new ArrayList<TestCase<? extends T>>(testCases);
        this.converters = converters;
    }

    // for use in this package

    void putAndCheck(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        converters.setUp(testCase);
        testCase.put(converters.linkedTarget());
        converters.syncConversions();
        if (converters.linkedSource().isNull()) {
            fail("source shouldn't be null: " + converters.linkedSource());
        }
        converters.checkPut(testCase.expectedState());
        testCase.check(converters.linkedSource());
    }

    void targetAlwaysAcceptsNull(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        converters.setUp(testCase);
        converters.linkedTarget().putNull();
        converters.syncConversions();
        if (!converters.linkedSource().isNull()) {
            fail("source should be null: " + converters.linkedSource());
        }
    }

    void getMismatch(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        AkType expectedType = testCase.type();
        converters.setUp(testCase);
        testCase.put(converters.linkedTarget());
        converters.syncConversions();

        TestCase<?> switched = resolveSwitcher(testCase);
        boolean gotError = false;
        try {
            switched.get(converters.linkedSource());
        } catch (WrongValueGetException t) {
            gotError = true;
        }
        if (!gotError) {
            fail(errorMessage("ValueSource", "getting", expectedType, switched));
        }
    }

    void setupUnsupported(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        converters.setUp(testCase);
    }

    void putUnsupported(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        testCase.put(converters.linkedTarget());
    }

    void getUnsupported(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        testCase.get(converters.linkedSource());
    }

    private String errorMessage(String failedClass, String action, AkType expectedType, TestCase<?> switched) {
        return "expected " + failedClass + " error after " + action + ' ' + switched
                + ": expected check for " + expectedType + " when " + action + ' ' + switched.type();
    }

    void putMismatch(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        AkType expectedType = testCase.type();
        converters.setUp(testCase);

        TestCase<?> switched = resolveSwitcher(testCase);
        boolean gotError = false;
        try {
            switched.put(converters.linkedTarget());
        } catch (WrongValueGetException t) {
            gotError = true;
        }
        if (!gotError) {
            fail(errorMessage("ValueTarget", "putting", expectedType, switched));
        }
    }

    private static TestCase<?> resolveSwitcher(TestCase<?> switcherTestCase) {
        Object state = switcherTestCase.expectedState();
        if (state instanceof MismatchedConversionsSuite.Switcher) {
            return ((MismatchedConversionsSuite.Switcher)state).switchTo();
        }
        throw new UnsupportedOperationException("not a switcher state: " + state);
    }

    List<String> testCaseNames() {
        List<String> names = new ArrayList<String>();
        for (TestCase<? extends T> testCase : testCases) {
            names.add(testCase.toString());
        }
        return names;
    }

    LinkedConversion<? super T> linkedConversion() {
        return converters;
    }

    // Object state

    private final List<TestCase<? extends T>> testCases;
    private final LinkedConversion<? super T> converters;

    // nested classes

    public static class SuiteBuilder<T> {

        public ConversionSuite<?> suite() {
            return new ConversionSuite<T>(converters, testCases);
        }

        public SuiteBuilder<T> add(TestCase<? extends T> testCase) {
            testCases.add(testCase);
            return this;
        }

        public SuiteBuilder(LinkedConversion<? super T> converters) {
            this.converters = converters;
            this.testCases = new ArrayList<TestCase<? extends T>>();
        }

        private final LinkedConversion<? super T> converters;
        private final List<TestCase<? extends T>> testCases;
    }
}
