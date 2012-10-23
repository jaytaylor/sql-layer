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

package com.akiban.server.types.util;

import com.akiban.server.error.InconvertibleTypesException;
import com.akiban.server.error.InvalidCharToNumException;
import com.akiban.server.types.AkType;
import static com.akiban.server.types.AkType.*;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.conversion.Converters;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.extract.ObjectExtractor;
import com.akiban.util.WrappingByteSource;
import com.google.common.collect.Iterables;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

public class ValueSources
{
    public static final Map<AkType, Map<AkType, Comparator<ValueSource>>> map;
    private static final long MULS[] = {60L * 60 * 1000 , 60 * 1000, 1000};
    
    static
    {
        map = new EnumMap<AkType, Map<AkType, Comparator<ValueSource>>>(AkType.class);
        
        // date 
        EnumSet<AkType> date = EnumSet.of(DATE, YEAR);
        // time
        EnumSet<AkType> time = EnumSet.of(TIME);
        // datetime, timestamp
        EnumSet<AkType> datetime = EnumSet.of(DATETIME, TIMESTAMP);
        // year
       // EnumSet<AkType> year = EnumSet.of(YEAR);
        // millis
        EnumSet<AkType> millis = EnumSet.of(INTERVAL_MILLIS);
        //month
        EnumSet<AkType> month = EnumSet.of(INTERVAL_MONTH);
        //bool
        EnumSet<AkType> bool = EnumSet.of(BOOL);
        // text
        EnumSet<AkType> text = EnumSet.of(TEXT, VARCHAR);
        // exact
        EnumSet<AkType> exact = EnumSet.of(U_INT, INT, LONG);
        // float
        EnumSet<AkType> floating = EnumSet.of(DOUBLE, U_DOUBLE, U_FLOAT, FLOAT);
        // bigInt
        EnumSet<AkType> bigInt = EnumSet.of(U_BIGINT);
        // bigDec
        EnumSet<AkType> bigDec = EnumSet.of(DECIMAL);
        
        // map 2)
       
        //doubleComparator
        Comparator<ValueSource> d_c2 = new Comparator<ValueSource>()
        {

            /**
             * Guarantee that right is bool/date/time/intervals/varchar
             */
            @Override
            public int compare(ValueSource left, ValueSource right)
            {
                // TODO: deal with overflow in U_BIGINT
                // actually, overflow implies inequality
                
                return Double.compare(Extractors.getDoubleExtractor().getDouble(left),
                                      Extractors.getDoubleExtractor().getDouble(right));
            }
            
        };
        
        // decimal comparator
        Comparator<ValueSource> dec_c2 = new Comparator<ValueSource>()
        {
            @Override
            public int compare(ValueSource l, ValueSource r)
            {                
                // BigDecimal is incapable of converting [Double|Float].[NaN | INFINITY] to BigDecimal. 
                // It'd throw NumberFormatException, instead.
                // So if those values appear, we'll compare them as Double                
                switch(l.getConversionType())
                {
                    case DOUBLE:
                    case U_DOUBLE:
                    case FLOAT:
                    case U_FLOAT:
                        double left = Extractors.getDoubleExtractor().getDouble(l);
                        if (Double.isInfinite(left) || Double.isNaN(left))
                            return Double.compare(left, r.getDecimal().doubleValue());
                }
                
                switch(r.getConversionType())
                {
                    case DOUBLE:
                    case U_DOUBLE:
                    case FLOAT:
                    case U_FLOAT:
                        double right = Extractors.getDoubleExtractor().getDouble(r);                                
                        if (Double.isInfinite(right) || Double.isNaN(right))
                            return Double.compare(right, l.getDecimal().doubleValue());
                    
                }
                
                ObjectExtractor<BigDecimal> extractor = Extractors.getDecimalExtractor();
                return extractor.getObject(l).compareTo(extractor.getObject(r));
            }                        
        };

        Map<AkType, Comparator<ValueSource>> m2Double = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        Map<AkType, Comparator<ValueSource>> m2Decimal = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        
        //TODO: add BOOL
        for (AkType right : Iterables.concat(floating, exact, date, time, datetime, month, millis, text))
        {
            m2Double.put(right, d_c2);
            m2Decimal.put(right, dec_c2);
        }

        for (AkType right : Iterables.concat(bigInt, bigDec))
        {
            m2Double.put(right, dec_c2);
            m2Decimal.put(right, dec_c2);
        }
        
        for (AkType left : Iterables.concat(floating, exact))
            map.put(left, m2Double);
        
        for (AkType left : Iterables.concat(bigInt, bigDec))
            map.put(left, m2Decimal);
    
        //------------------------------
        // map 3)
        Map<AkType, Comparator<ValueSource>> m3 = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        
        Comparator<ValueSource> l_c3 = new Comparator<ValueSource>()
        {

            @Override
            public int compare(ValueSource t, ValueSource t1)
            {
                LongExtractor extractor = Extractors.getLongExtractor(AkType.LONG);
                
                return (int)(extractor.getLong(t) - extractor.getLong(t1));
            }
            
        };
        
        for (AkType right : Iterables.concat(date, time, datetime, month, millis))
            m3.put(right, l_c3);
        map.put(BOOL, m3);
        
        //------------------------------
        // map 4)
        // null (throw incompatible)
        
        //------------------------------
        // map 5)
        // interval millis, time
        Map<AkType, Comparator<ValueSource>> m5 = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        
        m5.put(AkType.TIME, new Comparator<ValueSource>()
        {

            @Override
            public int compare(ValueSource left, ValueSource right)
            {
                // turn time into interval millis
                long ymd_hms[] = Extractors.getLongExtractor(AkType.TIME).getYearMonthDayHourMinuteSecond(right.getTime());
                
                long r = 0;
                for (int n = 3; n < 6; ++n)
                    r += ymd_hms[n] * MULS[n - 3];
                return (int)(left.getInterval_Millis() - r);
            }
            
        });
        
        map.put(INTERVAL_MILLIS, m5);
        
        //------------------------------
        // map 6)
        // null throw incompatible ?
        
        //------------------------------
        // map 7)
        // null throw incompatible
        

        //------------------------------
        // map 8), 9), 10) : left is VARCHAR
        Map<AkType, Comparator<ValueSource>> m8_9_10 = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        
        //8) cast both to double
        m8_9_10.put(BOOL, d_c2);
        m8_9_10.put(INTERVAL_MONTH, d_c2);
        
        // 9) varchar and millis
        m8_9_10.put(INTERVAL_MILLIS, new Comparator<ValueSource>()
        {
            @Override
            public int compare(ValueSource left, ValueSource right)
            {
                try
                {
                    double val = Double.parseDouble(Extractors.getStringExtractor().getObject(left));
                    return Double.compare(val, 
                            right.getInterval_Millis());
                }
                catch (NumberFormatException e)
                {
                    
                }
                
                // second attempt cast left (varchar) to time -> millis
                // then compare
                try
                {
                    LongExtractor extractor = Extractors.getLongExtractor(TIME);
                    
                    long ymd_hms[] = extractor.getYearMonthDayHourMinuteSecond(extractor.getLong(left));
                    long millis = 0;
                    for (int n = 3; n < 6; ++n)
                        millis += ymd_hms[n] * MULS[n - 3];
                    return (int)(millis - right.getInterval_Millis());
                }
                catch (Exception e)
                {
                    return 1;
                }
            }
            
        });
        
        // 10) [varhcar, date/tme]
        Comparator<ValueSource> c10 = new Comparator<ValueSource>()
        {

            @Override
            public int compare(ValueSource left, ValueSource right)
            {
                // attempt #`
                try
                {
                    double val = Extractors.getDoubleExtractor().getDouble(Extractors.getStringExtractor().getObject(left));
                    return Double.compare(val, 
                            Extractors.getLongExtractor(right.getConversionType()).getLong(right));
                }
                catch (InvalidCharToNumException e)
                {   
                }
                
                // attempt#2
                try
                {
                    LongExtractor extractor = Extractors.getLongExtractor(right.getConversionType());
                    
                    return (int)(extractor.getLong(Extractors.getStringExtractor().getObject(left))
                            - extractor.getLong(right));
                }
                catch (Exception e)
                {
                    return 1;
                }
            }
        };
        
        for (AkType right : Iterables.concat(date, time, datetime))
            m8_9_10.put(right, c10);
           
        map.put(VARCHAR, m8_9_10);
        map.put(TEXT, m8_9_10);
        
        // 11) (date/time)
        Comparator<ValueSource> c11 = new Comparator<ValueSource>()
        {
            /**
             * left is DATE | DATETIME , right is datetime/timestamp
             */
            @Override
            public int compare(ValueSource v1, ValueSource v2)
            {
                LongExtractor ext = Extractors.getLongExtractor(v2.getConversionType());
                return (int)(ext.getLong(v1) - ext.getLong(v2));
            }
            
        };
        
        Map<AkType, Comparator<ValueSource>> m11 = new EnumMap<AkType, Comparator<ValueSource>>(AkType.class);
        m11.put(DATETIME, c11);
        m11.put(TIMESTAMP, c11);
        
        map.put(DATE, m11);
        map.put(DATETIME, m11);
        map.put(TIMESTAMP, m11);
    }
    
    public static boolean equals (ValueSource v1, ValueSource v2, boolean textAsNumeric)
    {
        return equals(v1, v2, textAsNumeric, Converters.DEFAULT_CS);
    }
    
    public static long compare (ValueSource v1, ValueSource v2)
    {
        return compare(v1, v2, Converters.DEFAULT_CS);
    }
    
    /**
     * Compare two instances of ValueSource.
     * Equality is defined as:
     *        
     *        TODO:
     *          - when extra attributes [ie., timezone] becomes available
     *              , take that into account when comparing two dates/times
     *              For instance '12:00:00' in GMT should be equals to '19:00:00' in GMT +7
     * 
     *      - Two valuesources have different types:
     *          0] (Both have the same type)
     *              => do "regular comparison"
     * 
     *          1] (Both are nulls) 
     *              => return false; 
     * 
     *          2] (numeric, [boolean | date/time | intervals | varchar])
     *              => cast the non-numeric to DOUBLE and compare the two values
     *
     *          3] (boolean, [date/time | interval])
     *              => cast them all to LONG and do the comparison
     * 
     *          4] (interval_month, interval_millis)
     *              => Soln #1: Throw incompatible exception 
     *              => Soln #2: Compares the raw long value [not a good choice]
     * 
     *          5] (interval_millis, time)
     *              => Assume "TIME" means duration. Thus turn the value into millis_sec
     * 
     *          6] (interval_month, time)
     *              => depends on the choice for comparison btw the 2 intervals
     * 
     *          7] (intervals, date/time)
     *              => Throw incompatible Exception
     * 
     *          8] (varchar, [boolean | interval_month])
     *              => cast both to DOUBLE [to avoid losing precision] then do comparison
     * 
     *          9] (varchar, interval_millis)
     *              => attempt#1: cast varchar to DOUBLE and do comparison
     *                       if cast failed, goto attempt#2
     *                       else return return rst
     *              => attempt#2: cast varchar to TIME and do comparison
     *                       if cast failed, return false;
     *                       else return rst;
     * 
     *          10] (varhcar, date/tme)
     *              => attempt #1: cast varchar to DOUBLE and do comparison
     *                       if cast failed, go to attempt#2
     *                       else return rst
     *                      
     *              => attempt #2: cast varchar to the date/time
     *                       if cast failed, return false;
     *                       else return rst
     * 
     *          11] (date, time)
     *              => date/time are not compatible
     * 
     *          12] otherwise, return false
     * 
     * 
     * TODO: Support comparison between VARBINARY  and other types (VARCHAR)
     * 
     * @param v1
     * @param v2
     * @return 
     */
    public static boolean equals (ValueSource v1, ValueSource v2, boolean textAsNumeric, String charSet)
    {
        if (v1.isNull() || v2.isNull()) return false;
        try
        {
            boolean ret = compare(v1, v2, charSet) == 0;
            
            if (!ret && textAsNumeric && isText(v1.getConversionType())
                                      && isText(v2.getConversionType()))
                    // try compare as double
                    return Double.compare(Extractors.getDoubleExtractor().getDouble(v1),
                                          Extractors.getDoubleExtractor().getDouble(v2))
                            == 0;
                    // TODO: if test failed (again!) could try to compare v1, va2 
                    // as DATES  select field ('9-12-12', '0009-12-12', 2);
                    // or time, etc ...

            return ret;
        }
        catch (InconvertibleTypesException e)
        {
            return false;
        }
    }
    
    public static long compare (ValueSource left, ValueSource right, String charSet)
    {
        if (left.isNull())
            return right.isNull() ? 0 : -1;
        if (right.isNull())
            return 1;
        AkType l = left.getConversionType();
        AkType r = right.getConversionType();    
        
        boolean isLeft = true; 
        if (l == r || (isLeft = isText(l)) && isText(r))
            return compareSameType(left, right);
        else if (isLeft && r == AkType.VARBINARY) 
            try
            {
                return new WrappingByteSource(Extractors.getStringExtractor().getObject(left).getBytes(charSet))
                        .compareTo(right.getVarBinary());
            } 
            catch (UnsupportedEncodingException ex)
            {
                throw new UnsupportedOperationException(ex);
            }
        
        else if (l == AkType.VARBINARY && isText(r))
            try
            {
                return left.getVarBinary().compareTo(
                        new WrappingByteSource(Extractors.getStringExtractor().getObject(right).getBytes(charSet)));
            } catch (UnsupportedEncodingException ex)
            {
                throw new UnsupportedOperationException(ex);
            }
        else
        {
            Map<AkType, Comparator<ValueSource>> v;
            Comparator<ValueSource> c1 = null, c2 = null;

            if ((v = map.get(r)) != null)
                c1 = v.get(l);

            if ((v = map.get(l)) != null)
                c2 = v.get(r);

            if (c1 == null)
            {
                if (c2 == null)
                    throw new InconvertibleTypesException(l, r);
                else
                    return c2.compare(left, right);
            }
            else
                return -c1.compare(right, left);
        }
    }

    private static boolean isText(AkType t)
    {
        return t == AkType.VARCHAR || t == AkType.TEXT;
    }
    
    private static long compareSameType (ValueSource l, ValueSource r)
    {
        switch(l.getConversionType())
        {
            case DECIMAL:   return l.getDecimal().compareTo(r.getDecimal());
            case U_BIGINT:  return l.getUBigInt().compareTo(r.getUBigInt());
            case DOUBLE:    return Double.compare(l.getDouble(), r.getDouble());
            case U_DOUBLE:  return Double.compare(l.getUDouble(), r.getUDouble());
            case FLOAT:     return Float.compare(l.getFloat(), r.getFloat());
            case U_FLOAT:   return Float.compare(l.getUFloat(), r.getUFloat());
            case INT:       
            case U_INT:     
            case LONG:
            case DATE:
            case TIME:
            case DATETIME:
            case TIMESTAMP:
            case YEAR:      LongExtractor ex = Extractors.getLongExtractor(l.getConversionType());
                            return ex.getLong(l) - ex.getLong(r);
            case BOOL:      return Boolean.valueOf(l.getBool()).compareTo(r.getBool());
            case VARCHAR:
            case TEXT:      ObjectExtractor<String> ext = Extractors.getStringExtractor();
                            return ext.getObject(l).compareTo(ext.getObject(r));
            case NULL:      return 1;
            case VARBINARY: return l.getVarBinary().compareTo(r.getVarBinary());
            default:        throw new UnsupportedOperationException("Unsupported type: " + l.getConversionType());
    }
}
    
    // for testing
    static Comparator<ValueSource> get (AkType left, AkType right)
    {
        Map<AkType, Comparator<ValueSource>> v;
        Comparator<ValueSource> c1 = null, c2 = null;

        if ((v = map.get(right)) != null)
            c1 = v.get(left);

        if ((v = map.get(left)) != null)
            c2 = v.get(right);

        return c1 == null ? c2 : c1;
    }
}
