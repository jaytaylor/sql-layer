
package com.akiban.server.types.conversion;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.types.AkType;
import com.akiban.server.types.FromObjectValueSource;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.LinkedConversion;
import com.akiban.server.types.typestests.TestCase;
import org.junit.Assert;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

public final class ObjectConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new ObjectConversionLink()).suite();
        return params(suite);
    }

    public ObjectConversionTest(ConversionSuite<?> suite, int i) {
        super(suite, i);
    }

    private static class ObjectConversionLink implements LinkedConversion<Object> {
        @Override
        public ValueSource linkedSource() {
            return source;
        }

        @Override
        public ValueTarget linkedTarget() {
            return target;
        }

        @Override
        public void checkPut(Object expected) {
            Assert.assertEquals("last converted object", expected, target.lastConvertedValue());
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            target.expectType(testCase.type());
        }

        @Override
        public void syncConversions() {
            source.setExplicitly(target.lastConvertedValue(), target.getConversionType());
        }

        @Override
        public Set<? extends AkType> unsupportedTypes() {
            return Collections.emptySet();
        }

        private final FromObjectValueSource source = new FromObjectValueSource();
        private final ToObjectValueTarget target = new ToObjectValueTarget();
    }
}
