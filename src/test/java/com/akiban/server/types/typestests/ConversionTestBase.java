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

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public abstract class ConversionTestBase<T> {

    @Test
    public void putAndCheck() {
        linkedConversion.setUp(testCase.type());
        testCase.put(linkedConversion.target());
        linkedConversion.checkPut(testCase.expectedState());
        testCase.check(linkedConversion.source());
    }

    @Test
    public void targetAlwaysAcceptsNull() {
        linkedConversion.setUp(testCase.type());
        linkedConversion.target().putNull();
        assertTrue("source shoudl be null", linkedConversion.source().isNull());
    }

    protected ConversionTestBase(LinkedConversion<? super T> linkedConversion, TestCase<? extends T> testCase) {
        this.linkedConversion = linkedConversion;
        this.testCase = testCase;
    }

    private final LinkedConversion<? super T> linkedConversion;
    private final TestCase<? extends T> testCase;
}
