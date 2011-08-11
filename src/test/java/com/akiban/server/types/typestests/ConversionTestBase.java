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
import org.junit.Test;

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
        for (ConversionSuite<?> suite : suites) {
            List<String> names = suite.testCaseNames();
            for (int i=0; i < names.size(); ++i) {
                builder.add(names.get(i), suite, i);
            }
        }
        return builder.asList();
    }

    protected ConversionTestBase(ConversionSuite<?> suite, int indexWithinSuite) {
        this.suite = suite;
        this.indexWithinSuite = indexWithinSuite;
    }

    private final ConversionSuite<?> suite;
    private final int indexWithinSuite;
}
