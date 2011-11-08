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


import com.akiban.server.error.OverflowException;
import java.math.BigInteger;
import java.math.BigDecimal;
import com.akiban.server.aggregation.Aggregator;
import com.akiban.server.types.AkType;
import com.akiban.server.types.util.ValueHolder;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MinMaxSumAggregatorsTest 
{
    private final int MAX = 19;
    private ValueHolder holder = new ValueHolder();
    private Aggregator aggregator;

    // ----------------------test mins -----------------------------------------
    @Test
    public void testMinDouble ()
    {
        aggregator = Aggregators.mins("min", AkType.DOUBLE).get();
        assertEquals(0, aggregateDouble(), 0.01);
    }
    
    @Test
    public void testMinString ()
    {
       aggregator = Aggregators.mins("min", AkType.VARCHAR).get();
       assertEquals("0", aggregateString());      
    }
     
    @Test
    public void testMinLong ()
    {
        aggregator = Aggregators.mins("min", AkType.LONG).get();
        assertEquals(0, aggregateLong());
    }

    @Test
    public void testMinDecimal ()
    {
        aggregator = Aggregators.mins("min", AkType.DECIMAL).get();
        assertEquals(BigDecimal.ZERO, aggregateDecimal());
    }

    @Test
    public void testMinUBigInt ()
    {
        aggregator = Aggregators.mins("min", AkType.U_BIGINT).get();
        assertEquals(BigInteger.ZERO, aggregateBigInt());
    }
    
    @Test
    public void testMinDate ()
    {
        aggregator = Aggregators.mins("min",AkType.DATE).get();
        assertEquals(0, aggregateDate());
    }
    
    @Test
    public void testMinTime ()
    {
        aggregator = Aggregators.mins("min", AkType.TIME).get();
        assertEquals(0, aggregateTime());
    }
    
    @Test
    public void testMinBool ()
    {
        aggregator = Aggregators.mins("min", AkType.BOOL).get();
        assertEquals(false, aggregateBool());
    }

    @Test
    public void testMinString_oneRow ()
    {
        holder.clear();
        aggregator = Aggregators.mins("min", AkType.VARCHAR).get();

        holder.putString("z");
        aggregator.input(holder);
        aggregator.output(holder);

        assertEquals("z", holder.getString());
    }

    @Test
    public void testMinNullExpression ()
    {       
        aggregator = Aggregators.mins("min",AkType.DOUBLE).get();
        
        holder.putDouble(2.0);        
        aggregator.input(holder);

        holder.putNull();
        aggregator.input(holder);

        holder.putDouble(4.2);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals( new ValueHolder(AkType.DOUBLE, 2.0), holder);
    }
    
    @Test
    public void testMinWithInfinity ()
    {
        aggregator = Aggregators.mins("min", AkType.DOUBLE).get();
        
        holder.putDouble(2);
        aggregator.input(holder);
        
        holder.putDouble(Double.POSITIVE_INFINITY);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals(2.0, holder.getDouble(), 0.001);
    }

    // -----------------------test maxes ---------------------------------------
    @Test
    public void testMaxDouble ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.DOUBLE).get();
        assertEquals(MAX, aggregateDouble(), 0.01);
    }
    
    @Test
    public void testMaxString ()
    {
       aggregator = Aggregators.maxes("maxes", AkType.VARCHAR).get();
       assertEquals("9", aggregateString());      
    }
     
    @Test
    public void testMaxLong ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.LONG).get();
        assertEquals(MAX, aggregateLong());
    }

    @Test
    public void testMaxDecimal ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.DECIMAL).get();
        assertEquals(BigDecimal.valueOf(MAX), aggregateDecimal());
    }
   
    @Test
    public void testMaxUBigInt ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.U_BIGINT).get();
        assertEquals(BigInteger.valueOf(MAX), aggregateBigInt());
    }
    
    @Test
    public void testMaxDate ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.DATE).get();
        assertEquals(MAX, aggregateDate());
    }
    
    @Test
    public void testMaxTime ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.TIME).get();
        assertEquals(MAX, aggregateTime());
    }
    
    @Test
    public void testMaxBool ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.BOOL).get();
        assertEquals(true, aggregateBool());
    }

    @Test
    public void testMaxNullExpression ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.DOUBLE).get();
        holder.clear();

        holder.putNull();
        aggregator.input(holder);

        holder.putDouble(2.0);
        aggregator.input(holder);

        holder.putNull();
        aggregator.input(holder);

        holder.putDouble(0.0);
        aggregator.output(holder);
        assertEquals(2, holder.getDouble(), 0.01);
    }
      
    @Test
    public void testMaxWithInfinity ()
    {
        aggregator = Aggregators.maxes("maxes", AkType.DOUBLE).get();
        
        holder.putDouble(2);
        aggregator.input(holder);
        
        holder.putDouble(Double.NEGATIVE_INFINITY);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals(2.0, holder.getDouble(), 0.001);
        
    }
    // ------------------------------- test sum -------------------------------
    @Test
    public void testSumWithNull()
    {
        aggregator = Aggregators.sum("sum", AkType.LONG).get();
        long sum = 0;
        for (int n = 0; n < MAX; ++n)
        {
            holder.putLong(n);
            aggregator.input(holder);
            sum += n;
        }
        holder.putNull();
        aggregator.input(holder);

        holder.putLong(MAX);
        aggregator.input(holder);
        sum += MAX;

        aggregator.output(holder);

        assertEquals(holder.getLong(), sum);
    }
    
    @Test (expected = OverflowException.class)
    public void testSumWithDoubleOverflow ()
    {
        aggregator = Aggregators.sum("sum", AkType.DOUBLE).get();
        for (int n = 0; n < 3; ++n)
        {
              holder.putDouble(Double.MAX_VALUE);
              aggregator.input(holder);
        }
        aggregator.output(holder);
        assertEquals(Double.POSITIVE_INFINITY, holder.getDouble(), 0.001);
    }

    
    @Test // do not expect overflow
    public void testSumWithInfinity () 
    {
        aggregator = Aggregators.sum("sum", AkType.DOUBLE).get();
        
        
        for (int n = 0; n < 3; ++n)
        {
            holder.putDouble(n);
            aggregator.input(holder);
        }
        holder.putDouble(Double.POSITIVE_INFINITY);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals(Double.POSITIVE_INFINITY, holder.getDouble(), 0.001);
    }
    
    @Test // do not expect overflow, result is NaN
    public void testSumWithNegInfiAndPosInfi ()
    {
        aggregator = Aggregators.sum("sum", AkType.DOUBLE).get();
 
        holder.putDouble(Double.NEGATIVE_INFINITY);
        aggregator.input(holder);

        holder.putDouble(Double.POSITIVE_INFINITY);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals(Double.NaN, holder.getDouble(), 0.001);
    }
    
    @Test
    public void testSumWithNaN ()
    {
        aggregator = Aggregators.sum("sum", AkType.DOUBLE).get();
 
        holder.putDouble(2.3);
        aggregator.input(holder);

        holder.putDouble(Double.NaN);
        aggregator.input(holder);
        
        aggregator.output(holder);
        assertEquals(Double.NaN, holder.getDouble(), 0.001);
        
    }
    
    @Test(expected = UnsupportedOperationException.class)
    public void testSum ()
    {
        holder.clear();
        aggregator = Aggregators.sum("sum", AkType.VARCHAR).get();       
    }
   
    @Test
    public void testSumLong ()
    {
        holder.clear();
        aggregator = Aggregators.sum("sum", AkType.LONG).get();
        long sum = 0;

        for (int n = 0; n < MAX; ++n)
        {
            holder.putLong(n);
            aggregator.input(holder);
            sum =+ n;
        }
       assertEquals(sum, holder.getLong());
    }
    
    @Test (expected = OverflowException.class)
    public void testSumWithLongOverflow ()
    {
        holder.clear();
        aggregator = Aggregators.sum("sum", AkType.LONG).get();
        
        holder.putLong(Long.MAX_VALUE);
        aggregator.input(holder);
        aggregator.input(holder);
        
        aggregator.output(holder);
    }
    
    @Test (expected = OverflowException.class)
    public void testSumWithLongUnderflow()
    {
        holder.clear();
        aggregator = Aggregators.sum("sum", AkType.LONG).get();
        
        holder.putLong(Long.MIN_VALUE);
        aggregator.input(holder);
        aggregator.input(holder);
        
        aggregator.output(holder);
    }
 
    //--------------------------- private methods ------------------------------
    private double aggregateDouble ()
    {
        holder.clear(); 
        for (int n  = 0; n < 20; ++n)
        {
            holder.putDouble(n);
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getDouble();
    }
        
    private long aggregateLong()
    {
        holder.clear();
        for (int n  = 0; n < 20; ++n)
        {
            holder.putLong(n);
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getLong();
    }

    private BigDecimal aggregateDecimal ()
    {
        holder.clear();
        for (int n = 0; n < 20; ++n)
        {
            holder.putDecimal(BigDecimal.valueOf(n));            
            aggregator.input(holder);           
        }
        aggregator.output(holder);
        return holder.getDecimal();
    }
 
    private BigInteger aggregateBigInt ()
    {
        holder.clear();
        for (int n = 0; n < 20; ++n)
        {
            holder.putUBigInt(BigInteger.valueOf(n));
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getUBigInt();
    }
    
    private String aggregateString ()
    {
        holder.clear();
        for (int n = 0; n < 20; ++n)
        {
            holder.putString(n + "");
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getString();
    }
    
    private boolean aggregateBool ()
    {
        holder.clear();
        for (int n = 0; n < 20; ++n)
        {
            holder.putBool(n % 2 == 0);
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getBool();
    }

    private long aggregateDate()
    {
        holder.clear();
        for (int n  = 0; n < 20; ++n)
        {
            holder.putDate(n);
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getDate();
    }

    private long aggregateTime ()
    {
        holder.clear();
        for (int n = 0; n < 20; ++n)
        {
            holder.putTime(n);
            aggregator.input(holder);
        }
        aggregator.output(holder);
        return holder.getTime();
    }  
}
