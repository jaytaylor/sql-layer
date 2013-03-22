
package com.akiban.server.types.util;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.SimpleLinkedConversion;
import com.akiban.server.types.typestests.TestCase;

import java.util.Collection;

/**
 * Parameterised tests for ValueHolder
 */
public final class ValueHolderTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ValueHolderLink()).suite();
        return params(suite);
    }

    public ValueHolderTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }
    
    private static class ValueHolderLink extends SimpleLinkedConversion {
        @Override
        public ValueSource linkedSource() {
            return valueHolder;
        }

        @Override
        public ValueTarget linkedTarget() {
            return valueHolder;
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            valueHolder.requireForPuts(testCase.type());
        }

        private final ValueHolder valueHolder = new ValueHolder();
    }
}
