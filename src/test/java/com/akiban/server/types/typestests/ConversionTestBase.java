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

package com.akiban.server.types.typestests;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.OnlyIf;
import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.ValueTarget;
import com.akiban.server.types.extract.ConverterTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

@RunWith(NamedParameterizedRunner.class)
public abstract class ConversionTestBase {

    @Test
    @OnlyIf("isGood()")
    public void putAndCheck() {
        suite.putAndCheck(indexWithinSuite);
    }

    @Test
    @OnlyIf("isGood()")
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

    @Test(expected = UnsupportedOperationException.class)
    @OnlyIfNot("isSupported()")
    public void setupUnsupported() {
        suite.setupUnsupported(indexWithinSuite);
    }

    @Test(expected = UnsupportedOperationException.class)
    @OnlyIfNot("isSupported()")
    public void putUnsupported() {
        suite.putUnsupported(indexWithinSuite);
    }

    @Test(expected = UnsupportedOperationException.class)
    @OnlyIfNot("isSupported()")
    public void getUnsupported() {
        suite.getUnsupported(indexWithinSuite);
    }

    public boolean isMismatch() {
        Class<?> linkedConversionClass = suite.linkedConversion().getClass();
        return linkedConversionClass.equals(MismatchedConversionsSuite.DelegateLinkedConversion.class);
    }

    public boolean isGood() {
        return !(isMismatch()) && isSupported();
    }

    public boolean isSupported() {
        TestCase<?> testCase = suite.testCaseAt(indexWithinSuite);
        return ! suite.linkedConversion().unsupportedTypes().contains(testCase.type());
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

    protected static Collection<Parameterization> filter(
            Collection<Parameterization> collection,
            Predicate predicate)
    {
        for(Iterator<? extends Parameterization> iter = collection.iterator(); iter.hasNext(); ) {
            Parameterization param = iter.next();
            ConversionSuite<?> suite = (ConversionSuite) param.getArgsAsList().get(0);
            Integer indexWithinSuite = (Integer) param.getArgsAsList().get(1);
            if (!predicate.include(suite.testCaseAt(indexWithinSuite))) {
                iter.remove();
            }
        }
        return collection;
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
        public ValueTarget linkedTarget() {
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

        @Override
        public Set<? extends AkType> unsupportedTypes() {
            return delegate.unsupportedTypes();
        }

        NoCheckLinkedConversion(LinkedConversion<?> delegate) {
            this.delegate = delegate;
        }

        private final LinkedConversion<?> delegate;
    }

    protected interface Predicate {
        boolean include(TestCase<?> testCase);
    }
}
