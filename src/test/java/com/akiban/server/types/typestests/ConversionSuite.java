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

package com.akiban.server.types.typestests;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

public final class ConversionSuite<T> {

    public static <T> SuiteBuilder<T> build(LinkedConversion<? super T> converters) {
        return new SuiteBuilder<T>(converters);
    }

    public ConversionSuite(LinkedConversion<? super T> converters, List<TestCase<? extends T>> testCases) {
        this.testCases = new ArrayList<TestCase<? extends T>>(testCases);
        this.converters = converters;
    }

    // for use in this package

    void putAndCheck(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        converters.setUp(testCase.type());
        testCase.put(converters.linkedTarget());
        converters.syncConversions();
        converters.checkPut(testCase.expectedState());
        testCase.check(converters.linkedSource());
    }

    void targetAlwaysAcceptsNull(int i) {
        TestCase<? extends T> testCase = testCases.get(i);
        converters.setUp(testCase.type());
        converters.linkedTarget().putNull();
        converters.syncConversions();
        assertTrue("source shoudl be null", converters.linkedSource().isNull());
    }

    List<String> testCaseNames() {
        List<String> names = new ArrayList<String>();
        for (TestCase<? extends T> testCase : testCases) {
            names.add(testCase.toString());
        }
        return names;
    }

    // Object state

    private final List<TestCase<? extends T>> testCases;
    private final LinkedConversion<? super T> converters;

    // nested classes

    public static class SuiteBuilder<T> {

        public ConversionSuite<T> suite() {
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
