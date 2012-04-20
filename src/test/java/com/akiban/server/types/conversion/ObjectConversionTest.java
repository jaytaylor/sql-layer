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

import static org.junit.Assert.assertEquals;

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
