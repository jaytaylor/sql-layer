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
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        FLOAT
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        LONG
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        INT
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        DECIMAL
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        U_BIGINT
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
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
        },

        VARCHAR
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putString(n + "");
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        BOOL
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putBool(n % 2 == 0);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        DATE
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putDate(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        },

        TIME
        {
            public BigInteger aggregate(Aggregator aggregator, ValueHolder holder)
            {
                holder.clear();
                for (int n = 0; n < 20; ++n)
                {
                    holder.putTime(n);
                    aggregator.input(holder);
                }
                holder.putUBigInt(BigInteger.ONE);
                aggregator.output(holder);
                return holder.getUBigInt();
            }
        };

        abstract BigInteger aggregate (Aggregator aggregator, ValueHolder holder);
    }


    protected static final EnumSet<AkType> SUPPORTED = EnumSet.of(AkType.LONG,
                                                                AkType.DOUBLE,
                                                                AkType.INT,
                                                                AkType.U_BIGINT,
                                                                AkType.DECIMAL,
                                                                AkType.FLOAT);

    private static ValueHolder holder = new ValueHolder();
    private static final BigInteger N64 = new BigInteger("FFFFFFFFFFFFFFFF", 16);
    private static boolean alreadyExc;

    private Aggregator aggregator;
    private BigInteger expected;
    private Types type;

    public BitAggregatorsTest (Aggregator aggregator, BigInteger expected, Types type)
    {
        this.aggregator = aggregator;
        this.expected = expected;
        this.type = type;
    }

    @NamedParameterizedRunner.TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();

        for (AkType t: SUPPORTED)
        {
            param(pb, Aggregators.bit_and("bit_and", t).get(), BigInteger.ZERO, Types.valueOf(t.name()));
            param(pb, Aggregators.bit_or("bit_or", t).get(), BigInteger.valueOf(31), Types.valueOf(t.name()));
            param(pb, Aggregators.bit_xor("bit_xor", t).get(), BigInteger.ZERO, Types.valueOf(t.name()));
        }

        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, Aggregator agg, BigInteger expected, Types type)
    {
        pb.add(agg.toString() + type, agg, expected, type);
    }

    @Test
    public void test ()
    {
        assertEquals(expected, type.aggregate(aggregator, holder));
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullAnd ()
    {
        aggregator = Aggregators.bit_and("bit_and", AkType.U_BIGINT).get();
        expected = N64;
        testNull();
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullOr ()
    {
        aggregator = Aggregators.bit_or("bit_or", AkType.U_BIGINT).get();
        expected = BigInteger.ZERO;
        testNull();
    }

    @OnlyIfNot("alreadyExc()")
    @Test
    public void testNullXOr ()
    {
        aggregator = Aggregators.bit_xor("bit_xor", AkType.U_BIGINT).get();
        expected = BigInteger.ZERO;
        testNull();
        alreadyExc = true;
    }

    public boolean alreadyExc ()
    {
        return alreadyExc;
    }

    private void testNull ()
    {
        aggregator.input(NullValueSource.only());
        aggregator.output(holder);

        assertEquals(expected, holder.getUBigInt());
    }
}
