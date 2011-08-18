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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ConversionTarget;
import com.akiban.server.types.ConverterTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@RunWith(NamedParameterizedRunner.class)
public abstract class ConversionTestBase {

    @Test
    @OnlyIfNot("isMismatch()")
    public void putAndCheck() {
        suite.putAndCheck(indexWithinSuite);
    }

    @Test
    @OnlyIfNot("isMismatch()")
    public void targetAlwaysAcceptsNull() {
        suite.targetAlwaysAcceptsNull(indexWithinSuite);
    }

    @Test
    @OnlyIf("isMismatch()")
    public void getMismatch() {
        suite.getMismatch(indexWithinSuite);
    }

    @Test
    @OnlyIf("isMismatch()")
    public void putMismatch() {
        suite.putMismatch(indexWithinSuite);
    }

    public boolean isMismatch() {
        Class<?> linkedConversionClass = suite.linkedConversion().getClass();
        return linkedConversionClass.equals(MismatchedConversionsSuite.DelegateLinkedConversion.class);
    }

    protected static Collection<Parameterization> params(ConversionSuite<?>... suites) {
        ParameterizationBuilder builder = new ParameterizationBuilder();
        int count = 0;
        for (ConversionSuite<?> suite : normalize(suites)) {
            List<String> names = suite.testCaseNames();
            for (int i=0; i < names.size(); ++i) {
                builder.add(count + ": " + names.get(i), suite, i);
                ++count;
            }
        }
        return builder.asList();
    }

    protected ConversionTestBase(ConversionSuite<?> suite, int indexWithinSuite) {
        ConverterTestUtils.setGlobalTimezone("UTC");
        this.suite = suite;
        this.indexWithinSuite = indexWithinSuite;
    }

    private static Collection<ConversionSuite<?>> normalize(ConversionSuite<?>[] suites) {
        List<ConversionSuite<?>> list = new ArrayList<ConversionSuite<?>>();

        for (ConversionSuite<?> suite : suites) {
            // the suite itself
            list.add(suite);

            // standard conversions
            NoCheckLinkedConversion noCheckConversion = new NoCheckLinkedConversion(suite.linkedConversion());
            ConversionSuite.SuiteBuilder<Object> builder = ConversionSuite.build(noCheckConversion);
            for (TestCase<?> testCase : StandardTestCases.get()) {
                builder.add(testCase);
            }
            list.add(builder.suite());

            // mismatched conversions
            list.add(MismatchedConversionsSuite.basedOn(suite.linkedConversion()));
        }

        return list;
    }

    private final ConversionSuite<?> suite;
    private final int indexWithinSuite;

    private static class NoCheckLinkedConversion implements LinkedConversion<Object> {
        @Override
        public ValueSource linkedSource() {
            return delegate.linkedSource();
        }

        @Override
        public ConversionTarget linkedTarget() {
            return delegate.linkedTarget();
        }

        @Override
        public void checkPut(Object expected) {
            // nothing
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            delegate.setUp(testCase);
        }

        @Override
        public void syncConversions() {
            delegate.syncConversions();
        }

        NoCheckLinkedConversion(LinkedConversion<?> delegate) {
            this.delegate = delegate;
        }

        private final LinkedConversion<?> delegate;
    }


}
