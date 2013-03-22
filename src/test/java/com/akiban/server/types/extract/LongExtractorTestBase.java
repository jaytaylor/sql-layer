
package com.akiban.server.types.extract;

import org.junit.Assert;
import org.junit.Test;

abstract public class LongExtractorTestBase {
    static protected class TestElement {
        private final long longVal;
        private final String string;
        private final String expectedString;

        public TestElement(String s, Number n) {
            this(s, n, s);

        }

        public TestElement(String inputString, Number encodedNumber, String decodedString) {
            this.longVal = encodedNumber.longValue();
            this.string = inputString;
            this.expectedString = decodedString;
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
    public final void decodingToString() {
        int count = 0;
        for(TestElement t : TEST_CASES) {
            final String decoded = ENCODER.asString(t.longVal);
            Assert.assertEquals("test case [" + count + "] long->String: " + t, t.expectedString, decoded);
            ++count;
        }
    }

    @Test
    public final void encodeAndDecodeAll() {
        for(TestElement t : TEST_CASES) {
            final String actual = encodeAndDecode(t.string);
            Assert.assertEquals("encodeAndDecode " + t, t.expectedString, actual);
        }
    }
}
