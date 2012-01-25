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

package com.akiban.server.types.extract;

import org.junit.Assert;
import org.junit.Test;

abstract public class LongExtractorTestBase {
    static protected class TestElement {
        private final long longVal;
        private final String string;

        public TestElement(String s, Number n) {
            this.longVal = n.longValue();
            this.string = s;
        }

        @Override
        public String toString() {
            return String.format("(%d, %s)", longVal, string);
        }
    }

    protected final LongExtractor ENCODER;
    protected final TestElement[] TEST_CASES;

    public LongExtractorTestBase(LongExtractor encoder, TestElement[] testElements) {
        this.ENCODER = encoder;
        this.TEST_CASES = testElements;
    }

    protected String encodeAndDecode(String str) {
        final long val = ENCODER.getLong(str);
        return ENCODER.asString(val);
    }

    @Test
    public void decodingToString() {
        int count = 0;
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.asString(t.longVal);
            Assert.assertEquals("test case [" + count + "] long->String: " + t, t.string, decoded);
            ++count;
        }
    }
}
