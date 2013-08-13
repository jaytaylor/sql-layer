/**
 * Copyright (C) 2009-2013 Akiban Technologies, Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.foundationdb.server.types.util;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.extract.Extractors;
import static com.foundationdb.server.types.AkType.*;
import com.foundationdb.server.types.ValueSource;
import com.foundationdb.server.types.extract.ConverterTestUtils;
import static org.junit.Assert.*;
import java.util.Collection;
import java.util.EnumSet;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Iterables;
import java.math.BigDecimal;
import java.math.BigInteger;

@RunWith(NamedParameterizedRunner.class)
public class ValueSourcesTest
{
    private static interface Op
    {
        boolean exc(ValueSource left, ValueSource right);
    }
    
    private static final int EQ = 0, GE = 1, GT = 2, LE = 3, LT = 4, NE = 5;
    private static final Op[] OPS = new Op[]
    {
        new Op(){public boolean exc(ValueSource a, ValueSource b){return ValueSources.equals(a, b, false);}},
        new Op(){public boolean exc(ValueSource a, ValueSource b){return ValueSources.compare(a, b) >= 0;}},
        new Op(){public boolean exc(ValueSource a, ValueSource b){return ValueSources.compare(a, b) > 0;}},
        new Op(){public boolean exc(ValueSource a, ValueSource b){return ValueSources.compare(a, b) <= 0;}},
        new Op(){public boolean exc(ValueSource a, ValueSource b){return ValueSources.compare(a, b) < 0;}},
        new Op(){public boolean exc(ValueSource a, ValueSource b){return !ValueSources.equals(a, b, false);}},
    };

    
    private static EnumSet<AkType> DATES = EnumSet.of(DATE, DATETIME, TIME, YEAR, TIMESTAMP);
    private static EnumSet<AkType> NUMS = EnumSet.of(DECIMAL, DOUBLE, FLOAT, U_FLOAT, U_DOUBLE, U_BIGINT, INT, U_INT);
    private static EnumSet<AkType> FLOATING = EnumSet.of(DECIMAL, DOUBLE, FLOAT, U_FLOAT, U_DOUBLE);
    private static EnumSet<AkType> EXACT = EnumSet.of(LONG, U_INT, INT, U_BIGINT);
    private static EnumSet<AkType> TEXTS = EnumSet.of(AkType.TEXT, VARCHAR);
    private static EnumSet<AkType> BOOLEAN = EnumSet.of(BOOL);
    private static EnumSet<AkType> INTERVALS = EnumSet.of(INTERVAL_MONTH, INTERVAL_MILLIS);
    
    private ValueHolder left;
    private ValueHolder right;
    int op;
    boolean result;

    public ValueSourcesTest(ValueHolder l, int op, ValueHolder r, boolean result)
    {
        left = l;
        right = r;
        this.op = op;
        this.result = result;
    }

    private static ValueHolder get(AkType t, double v)
    {
        switch (t)
        {
            case DECIMAL:
                return new ValueHolder(t, BigDecimal.valueOf(v));
            case DOUBLE:
            case U_DOUBLE:
                return new ValueHolder(t, v);
            case U_FLOAT:
            case FLOAT:
                return new ValueHolder(t, (float) v);
            case U_INT:
            case INT:
            case LONG:
            case DATE:
            case TIME:
            case YEAR:
            case DATETIME:
            case INTERVAL_MILLIS:
            case INTERVAL_MONTH:
            case TIMESTAMP:
                return new ValueHolder(t, (long) v);
            case BOOL:
                return new ValueHolder(t, ((int) v) != 0);
            case U_BIGINT:
                return new ValueHolder(t, BigInteger.valueOf((long) v));
            case TEXT:
            case VARCHAR:
                return new ValueHolder(t, Double.toString(v));
            default:
                return ValueHolder.holdingNull();

        }
    }

    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        // set timezone to UTC
        ConverterTestUtils.setGlobalTimezone("UTC");
        
        // special cases
        param(pb, new ValueHolder(VARCHAR, "00.1"), EQ, new ValueHolder(VARCHAR, "00.1"), true);
        param(pb, new ValueHolder(VARCHAR, "00.1"), EQ, new ValueHolder(VARCHAR, "0.1"), false);
        
        // MAP 2) {numeric, (DATE | TIME | NUMERICS | BOOL | TEXT)}
        for (AkType l : Iterables.concat(DATES, TEXTS, INTERVALS)) // TODO: add BOOL
        {
            for (AkType r : FLOATING)
            {
                paramSym(pb, get(l, 5), EQ, get(r, 5), true);
                paramSym(pb, get(l, 4), EQ, get(r, 4.2), false);
            }
            
            for (AkType r : EXACT)
                paramSym(pb, get(l, 3), EQ, get(r, 3), true);
        }
        

        for (AkType l : FLOATING)
        {
            for (AkType r : FLOATING)
                param(pb, get(l, 6.5), EQ, get(r, 6.5), true);
            

            for (AkType r : EXACT)
            {
                paramSym(pb, get(l, 6.5), EQ,get(r, 6), false);
                paramSym(pb, get(l, 6.0), EQ, get(r, 6), true);
            }
        }
       
        for (AkType l : EXACT)
            for (AkType r : EXACT)
                param(pb, get(l, 10), EQ,get(r, 10), true);
        
        // MAP 3) {boolean, (date/time | interval)}
        // TODO: BOOLEAN isn't convertible to anything other than itself and VARCHAR
//        for (AkType right : Iterables.concat(DATES, INTERVALS))
//        {
//            paramSym(pb, get(BOOL, 1), get(right, 5), false);
//            paramSym(pb, get(BOOL, 1), get(right, 1), true);
//        }

        // MAP 4) {interval_month, interval_millis}
        paramSym(pb, get(INTERVAL_MONTH, 1), EQ,get(INTERVAL_MILLIS, 1), false);

        // MAP 5) {interval_millis, time}
        // 2000 millis == TIME('00:00:02')
        paramSym(pb, get(INTERVAL_MILLIS, 2000), EQ,get(TIME, 000002), true);
        
        // MAP 6) {interval_month, time}
        // incompatible => simple false
        paramSym(pb, get(INTERVAL_MONTH, 1), EQ,get(TIME, 1), false);
        
        // MAP 7) {intervals, date/time}
        for (AkType left : INTERVALS)
            for (AkType right : DATES)
                if (right == TIME) continue; // skip, already tested
                else paramSym(pb, get(left, 1), EQ,get(right, 1), false); // incompatible => false
        
        // MAP 8) {varchar, (boolean | interval_month)}
        // (VARCHAR, BOOL)
        // TODO: BOOL isn't 'compatible' to anything yet
//        paramSym(pb, get(BOOL, 1), new ValueHolder(VARCHAR, "1"), true);
//        paramSym(pb, get(BOOL, 0), new ValueHolder(VARCHAR, "0"), true);
//        paramSym(pb, get(BOOL, 3), new ValueHolder(VARCHAR, "5"), false);
        
        // (VARCHAR, INTERVAL_MONTH)
        paramSym(pb, get(INTERVAL_MONTH, 1), EQ,get(VARCHAR, 1), true);
        paramSym(pb, get(INTERVAL_MONTH, 1), EQ,get(VARCHAR, 1.5), false);
   
        // MAP 9) {varchar, interval_millis}
        paramSym(pb, get(VARCHAR, 1), EQ,get(INTERVAL_MILLIS, 1), true);
        paramSym(pb, get(INTERVAL_MILLIS, 1000), EQ,new ValueHolder(VARCHAR, "00:00:01"), true);
     
        // MAP 10) {varchar, date/tme}
        for (AkType l: TEXTS)
            for (AkType r : DATES)
                paramSym(pb, get(l, 12345), EQ,get(r, 12345), true);
        
        paramSym(pb, get(TIME, 123010), EQ,new ValueHolder(VARCHAR, "12:30:10"), true);
        
          
        // MAP 11) {date, time}
        // ilegal comparisons
        for (AkType left : new AkType[]{TIMESTAMP, DATE, DATETIME, YEAR, TIME})
            for (AkType right : new AkType[] {YEAR, TIME})
                if (left == right) continue;
                else param(pb, get(left, 1), EQ,get(right, 1), false); // incompatible

        // legal 
        for (AkType dt : new AkType[]{DATETIME, TIMESTAMP})
        {
            paramSym(pb, new ValueHolder(DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2009-12-12"))
                       , EQ
                       , new ValueHolder(dt, Extractors.getLongExtractor(dt).getLong("2009-12-12 00:00:00"))
                       , true);
            
            paramSym(pb, new ValueHolder(DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"))
                       , EQ
                       , new ValueHolder(dt, Extractors.getLongExtractor(dt).getLong("2006-11-07 00:00:01"))
                       , false);

            param(pb, new ValueHolder(DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"))
                       , GT
                       , new ValueHolder(dt, Extractors.getLongExtractor(dt).getLong("2006-11-07 00:00:01"))
                       , false);
            
            param(pb, new ValueHolder(DATE, Extractors.getLongExtractor(AkType.DATE).getLong("2006-11-07"))
                       , LT
                       , new ValueHolder(dt, Extractors.getLongExtractor(dt).getLong("2006-11-07 00:00:01"))
                       , true);
        }
        
        paramSym(pb, new ValueHolder(DATETIME, 20061107123010L)
                   , EQ
                   , new ValueHolder(TIMESTAMP, Extractors.getLongExtractor(AkType.TIMESTAMP).getLong("2006-11-07 12:30:10"))
                   , true);
        
        //--------------- test HUGE number -------------------------------------
        param(pb, new ValueHolder(AkType.LONG,20081012000000L), LT, new ValueHolder(AkType.LONG, 20110504000000L), true);
//        param(pb, new ValueHolder(AkType.U_BIGINT, BigInteger.valueOf(Long.MAX_VALUE).pow(Integer.MAX_VALUE))
//                , EQ
//                , new ValueHolder(AkType.DOUBLE, 1.2)
//                , false);
        param(pb, new ValueHolder(AkType.DOUBLE, Double.POSITIVE_INFINITY), EQ, new ValueHolder(AkType.DECIMAL, BigDecimal.ZERO), false);
        return pb.asList();
    }

    private static void param(ParameterizationBuilder pb, ValueHolder l, int op, ValueHolder r, boolean expected)
    {
        pb.add("[" + l.getConversionType() + ": " + l + ", " + r.getConversionType() + ": " + r + "]" + op, l, op, r, expected);
    }

    private static void paramSym(ParameterizationBuilder pb, ValueHolder l, int op, ValueHolder r, boolean expected)
    {
        param(pb, l, op, r, expected);
        param(pb, r, op, l, expected);
    }

    @Test
    public void test()
    {
        assertEquals(result, OPS[op].exc(left, right));
    }
}
