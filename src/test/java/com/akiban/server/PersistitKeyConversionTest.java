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

package com.akiban.server;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.Parameterization;
import com.akiban.server.collation.AkCollatorFactory;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ToObjectValueTarget;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.typestests.ConversionSuite;
import com.akiban.server.types.typestests.ConversionTestBase;
import com.akiban.server.types.typestests.SimpleLinkedConversion;
import com.akiban.server.types.typestests.TestCase;
import com.persistit.Key;
import com.persistit.Persistit;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Set;

public final class PersistitKeyConversionTest extends ConversionTestBase {

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params() {
        ConversionSuite<?> suite = ConversionSuite.build(new KeyConversionPair()).suite();

        Collection<Parameterization> params = params(suite);

        // Persistit truncates trailing 0s from BigDecimals, which reduces their precision.
        // This is wrong, but that's what we have to deal with. So we'll ignore all such test cases.
        ToObjectValueTarget valueTarget = new ToObjectValueTarget();
        for (Iterator<Parameterization> iterator = params.iterator(); iterator.hasNext(); ) {
            Parameterization param = iterator.next();
            ConversionSuite<?> paramSuite = (ConversionSuite<?>) param.getArgsAsList().get(0);
            int indexWithinSuite = (Integer) param.getArgsAsList().get(1);
            TestCase<?> testCase = paramSuite.testCaseAt(indexWithinSuite);
            if (testCase.type().equals(AkType.DECIMAL)) {
                valueTarget.expectType(AkType.DECIMAL);
                testCase.put(valueTarget);
                BigDecimal expected = (BigDecimal) valueTarget.lastConvertedValue();
                String asString = expected.toPlainString();
                if (asString.contains(".") && asString.charAt(asString.length() - 1) == '0') {
                    iterator.remove();
                }
            }
        }
        return params;
    }

    public PersistitKeyConversionTest(ConversionSuite<?> suite, int indexWithinSuite) {
        super(suite, indexWithinSuite);
    }

    private static final class KeyConversionPair extends SimpleLinkedConversion {
        @Override
        public ValueSource linkedSource() {
            return source;
        }

        @Override
        public ValueTarget linkedTarget() {
            return target;
        }

        @Override
        public Set<? extends AkType> unsupportedTypes() {
            return EnumSet.of(AkType.INTERVAL_MILLIS, AkType.INTERVAL_MONTH);
        }

        @Override
        public void setUp(TestCase<?> testCase) {
            key.clear();
            target.attach(key);
            target.expectingType(testCase.type(), AkCollatorFactory.getAkCollator(testCase.charset()));
            source.attach(key, 0, testCase.type());
        }

        private final Key key = new Key((Persistit)null);
        private final PersistitKeyValueTarget target = new PersistitKeyValueTarget();
        private final PersistitKeyValueSource source = new PersistitKeyValueSource();
    }
}
