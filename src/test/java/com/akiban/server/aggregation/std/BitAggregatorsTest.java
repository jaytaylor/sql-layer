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

package com.akiban.server.aggregation.std;

import com.akiban.junit.OnlyIfNot;
import com.akiban.server.types.NullValueSource;
import java.util.EnumSet;
import com.akiban.junit.Parameterization;
import java.util.Collection;
import com.akiban.junit.ParameterizationBuilder;
import org.junit.runner.RunWith;
import com.akiban.junit.NamedParameterizedRunner;
import java.math.BigInteger;
import java.math.BigDecimal;
import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

@RunWith(NamedParameterizedRunner.class)
public class BitAggregatorsTest
{
    protected static enum Types
    {
        DOUBLE
        {
            @Override
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putDouble(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE); // just to change the type of holder to u_bigint
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            @Override
            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putDouble(5);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE); 
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        FLOAT
        {
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putFloat(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putFloat(5);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        LONG
        {
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putLong(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putLong(5);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        INT
        {
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putInt(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putInt(5);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        DECIMAL
        {
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putDecimal(BigDecimal.valueOf(n));
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putDecimal(BigDecimal.valueOf(5));
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        U_BIGINT
        {
            public BigInteger aggregate_test1(Aggregator aggregator, ValueHolder holder)
            {

                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putUBigInt(BigInteger.valueOf(n));
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }

            public BigInteger aggregate_test2(Aggregator aggregator, ValueHolder holder)
            {

                holder.clear();
                for (int n = 0; n < 5; ++n)
                {
                    holder.putUBigInt(BigInteger.valueOf(5));
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        };

        abstract BigInteger aggregate_test1 (Aggregator aggregator, ValueHolder holder);
        abstract BigInteger aggregate_test2 (Aggregator aggregator, ValueHolder holder);
    }

    protected static final EnumSet<AkType> SUPPORTED = EnumSet.of(AkType.LONG,
                                                                AkType.DOUBLE,
                                                                AkType.INT,
                                                                AkType.U_BIGINT,
                                                                AkType.DECIMAL,
                                                                AkType.FLOAT);

    private static ValueHolder holder = new ValueHolder();
    private static final BigInteger N64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
    private static boolean alreadyExc = false;

    private Aggregator aggregator;
    private BigInteger expected1;
    private BigInteger expected2;
    private Types type;

    public BitAggregatorsTest (Aggregator aggregator, BigInteger expected1, BigInteger expected2, Types type)
    {
        this.aggregator = aggregator;
        this.expected1 = expected1;
        this.expected2 = expected2;
        this.type = type;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        BigInteger bigInt5 = BigInteger.valueOf(5);
        for (AkType t: SUPPORTED)
        {
            param(pb, Aggregators.bit_and("bit_and", t).get(), BigInteger.ZERO, bigInt5, Types.valueOf(t.name()));
            param(pb, Aggregators.bit_or("bit_or", t).get(), BigInteger.valueOf(31), bigInt5, Types.valueOf(t.name()));
            param(pb, Aggregators.bit_xor("bit_xor", t).get(), BigInteger.ZERO, bigInt5, Types.valueOf(t.name()));
        }

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, Aggregator agg, BigInteger expected1,
            BigInteger expected2, Types type)
    {
        pb.add(agg.toString() + type, agg, expected1, expected2, type);
    }

    @Test
    public void testColumnOfDifferentValues ()
    {
        assertEquals(expected1, type.aggregate_test1(aggregator, holder));
    }

    @Test
    public void testColumnOfSameValues ()
    {
        assertEquals(expected2, type.aggregate_test2(aggregator, holder));
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullAnd ()
    {
        aggregator = Aggregators.bit_and("bit_and", AkType.U_BIGINT).get();
        expected1 = N64;
        testNull();
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testAndOrXor()
    {
        // test and
        aggregator = Aggregators.bit_and("bit_and", AkType.U_BIGINT).get();
        expected1 = BigInteger.valueOf(1);
        testNonParam();
        
        // test or
        aggregator = Aggregators.bit_or("bit_or", AkType.U_BIGINT).get();
        expected1 = BigInteger.valueOf(3);
        testNonParam();
        
        // test xor
        aggregator = Aggregators.bit_xor("bit_xor", AkType.U_BIGINT).get();
        expected1 = BigInteger.valueOf(2);
        testNonParam();
    }
    
    private void testNonParam ()
    {
        holder.clear();
        
        holder.putUBigInt(BigInteger.ONE);
        aggregator.input(holder);
        
        holder.putUBigInt(BigInteger.valueOf(3));
        aggregator.input(holder);
        
        holder.clear();
        holder.putUBigInt(N64);
        aggregator.output(holder);
        
        assertEquals(expected1, holder.getUBigInt());
    }
    
    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullOr ()
    {
        aggregator = Aggregators.bit_or("bit_or", AkType.U_BIGINT).get();
        expected1 = BigInteger.ZERO;
        testNull();
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullXOr ()
    {
        aggregator = Aggregators.bit_xor("bit_xor", AkType.U_BIGINT).get();
        expected1 = BigInteger.ZERO;
        testNull();
        alreadyExc = true;
    }

    public boolean alreadyExc ()
    {
        return alreadyExc;
    }

    private void testNull ()
    {
        holder.clear();
        aggregator.input(NullValueSource.only());
        holder.expectType(AkType.U_BIGINT);
        aggregator.output(holder);

        assertEquals(expected1, holder.getUBigInt());
    }
}
