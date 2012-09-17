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

package com.akiban.server.service.functions;

import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.aggregation.AggregatorFactory;
import com.akiban.server.error.AkibanInternalException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.types.AkType;
import com.akiban.sql.StandardException;
import com.akiban.server.expression.TypesList;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public final class FunctionsRegistryImplTest {
    
    @Test
    public void findAggregatorFactory() {
        FunctionsRegistryImpl registry = registry(Good.class);
        assertEquals(expectedAggregatorFactories(), registry.getAllAggregators());
        assertEquals("afoo", AGGREGATOR_FACTORY, registry.get("afoo", AkType.LONG));
        assertEquals("AFOO", AGGREGATOR_FACTORY, registry.get("AFOO", AkType.LONG));
    }

    @Test
    public void findExpressionComposer() {
        FunctionsRegistryImpl registry = registry(Good.class);
        assertEquals(expectedExpressionFactories(), registry.getAllComposers());
        assertEquals("foo", GOOD_EXPRESSION_COMPOSER, registry.composer("foo"));
        assertEquals("FOO", GOOD_EXPRESSION_COMPOSER, registry.composer("FOO"));
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void scalarWrongType() {
        registry(ScalarWrongType.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void scalarNotPublic() {
        registry(ScalarNotPublic.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void scalarNotFinal() {
        registry(ScalarNotFinal.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void scalarNotStatic() {
        registry(ScalarNotStatic.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void duplicateScalarAggregate() {
        registry(ScalarDuplicate.class, AggDuplicate.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void aggregateNotStatic() {
        registry(AggNotStatic.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void aggregateNotPublic() {
        registry(AggNotPublic.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void aggregateWrongRetValue() {
        registry(AggWrongReturnValue.class);
    }

    @Test(expected = FunctionsRegistryImpl.FunctionsRegistryException.class)
    public void aggregateWrongArgs() {
        registry(AggWrongArgs.class);
    }

    @Test(expected = AkibanInternalException.class)
    public void aggregateThrowsException() {
        registry(AggThrowsException.class);
    }

    // use in this class

    private static FunctionsRegistryImpl registry(Class<?>... classes) {
        return new FunctionsRegistryImpl(new InternalClassFinder(classes));
    }

    private static AggregatorFactory aggregatorFactoryMethod(String name, AkType type) {
        assert name != null;
        return type == AkType.LONG ? AGGREGATOR_FACTORY : null;
    }

    public static final ExpressionComposer GOOD_EXPRESSION_COMPOSER = new ExpressionComposer() {
        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Expression compose(List<? extends Expression> arguments, List<ExpressionType> typesList)
        {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public NullTreating getNullTreating()
        {
            return NullTreating.RETURN_NULL;
        }
    };

    static Map<String, Map<AkType, AggregatorFactory>> expectedAggregatorFactories() {
        Map<String,Map<AkType,AggregatorFactory>> expected = new HashMap<String, Map<AkType, AggregatorFactory>>();
        Map<AkType,AggregatorFactory> expectedInner = new EnumMap<AkType, AggregatorFactory>(AkType.class);
        expectedInner.put(AkType.LONG, AGGREGATOR_FACTORY);
        expected.put("afoo", expectedInner);
        return expected;
    }

    static Map<String,ExpressionComposer> expectedExpressionFactories() {
        return Collections.singletonMap("foo", GOOD_EXPRESSION_COMPOSER);
    }

    // class state

    private static final AggregatorFactory AGGREGATOR_FACTORY = new AggregatorFactory() {
        @Override
        public Aggregator get() {
            throw new UnsupportedOperationException();
        }
        @Override
        public String getName () {
            return "TEST";
        }
        @Override
        public AkType outputType() {
            return AkType.NULL;
        }

        @Override
        public Aggregator get(Object obj)
        {
            return get();
        }
    };

    // nested classes

    private static class InternalClassFinder implements FunctionsClassFinder {
        private InternalClassFinder(Class<?>... classes) {
            this.classes = new HashSet<Class<?>>(Arrays.asList(classes));
        }

        @Override
        public Set<Class<?>> findClasses() {
            return classes;
        }

        private final Set<Class<?>> classes;
    }

    // good example

    public static class Good {
        @Aggregate("AFOO") // note, should be converted to lowercase
        public static AggregatorFactory get(String name, AkType type) {
            return aggregatorFactoryMethod(name, type);
        }

        @Scalar("FOO") @SuppressWarnings("unused") // note, should be converted to lowercase
        public static final ExpressionComposer COMPOSER = GOOD_EXPRESSION_COMPOSER;
    }

    // bad scalars

    public static class ScalarWrongType {
        @Scalar("blah") @SuppressWarnings("unused")
        public static final Boolean WRONG = false;
    }

    public static class ScalarNotPublic {
        @Scalar("blah") @SuppressWarnings("unused")
        static final ExpressionComposer COMPOSER = GOOD_EXPRESSION_COMPOSER;
    }

    public static class ScalarNotFinal {
        @Scalar("blah") @SuppressWarnings("unused")
        public static ExpressionComposer COMPOSER = GOOD_EXPRESSION_COMPOSER;
    }

    public static class ScalarNotStatic {
        @Scalar("blah") @SuppressWarnings("unused")
        public final ExpressionComposer COMPOSER = GOOD_EXPRESSION_COMPOSER;
    }

    public static class ScalarDuplicate {
        @Scalar("foo") @SuppressWarnings("unused")
        public static final ExpressionComposer COMPOSER_A = GOOD_EXPRESSION_COMPOSER;
    }

    // bad aggregates

    public static class AggNotStatic {
        @Aggregate("foo") @SuppressWarnings("unused")
        public AggregatorFactory get(String name, AkType type) {
            return null;
        }
    }

    public static class AggNotPublic {
        @Aggregate("foo") @SuppressWarnings("unused")
        static AggregatorFactory get(String name, AkType type) {
            return null;
        }
    }

    public static class AggWrongReturnValue {
        @Aggregate("foo") @SuppressWarnings("unused")
        public Boolean get(String name, AkType type) {
            return null;
        }
    }

    public static class AggWrongArgs {
        @Aggregate("foo") @SuppressWarnings("unused")
        public Boolean get(String name, AkType type, Integer i) {
            return null;
        }
    }

    public static class AggDuplicate {
        @Aggregate("foo") @SuppressWarnings("unused")
        public static AggregatorFactory getA(String name, AkType type) {
            return null;
        }
    }
    public static class AggThrowsException {
        @Aggregate("foo") @SuppressWarnings("unused")
        public static AggregatorFactory get(String name, AkType type) {
            throw new UnsupportedOperationException();
        }
    }

}
