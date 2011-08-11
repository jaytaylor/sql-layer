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

import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ConversionSource;
import com.akiban.server.types.ConversionTarget;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class ConversionTestBase {

    @Test
    public void putAndCheck() {
        suite.putAndCheck(indexWithinSuite);
    }

    @Test
    public void targetAlwaysAcceptsNull() {
        suite.targetAlwaysAcceptsNull(indexWithinSuite);
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
        this.suite = suite;
        this.indexWithinSuite = indexWithinSuite;
    }

    private static Collection<ConversionSuite<?>> normalize(ConversionSuite<?>[] suites) {
        List<ConversionSuite<?>> list = new ArrayList<ConversionSuite<?>>();

        for (ConversionSuite<?> suite : suites) {
            list.add(suite);
            NoCheckLinkedConversion noCheckConversion = new NoCheckLinkedConversion(suite.linkedConversion());
            ConversionSuite.SuiteBuilder<Object> builder = ConversionSuite.build(noCheckConversion);
            for (TestCase<?> testCase : StandardTestCases.get()) {
                builder.add(testCase);
            }
            list.add(builder.suite());
        }

        return list;
    }

    private final ConversionSuite<?> suite;
    private final int indexWithinSuite;

    private static class NoCheckLinkedConversion implements LinkedConversion<Object> {
        @Override
        public ConversionSource linkedSource() {
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
        public void setUp(AkType type) {
            delegate.setUp(type);
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
