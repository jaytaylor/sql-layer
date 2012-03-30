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

import com.akiban.server.types.extract.LongExtractor;
import com.akiban.server.types.AkType;
import com.akiban.server.types.ValueSource;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.extract.ObjectExtractor;
import com.google.common.collect.Iterables;
import java.math.BigDecimal;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;

import static com.akiban.server.types.AkType.*;

public class ValueSources
{
   
    
    private static final Map<AkType, Map<AkType, Comparator<ValueSource>>> map;
    private static final long MULS[] = {60L * 60 * 1000 , 60 * 1000, 1000};
    
    static
    {
        map = new EnumMap(AkType.class);
        
        // date 
        EnumSet<AkType> date = EnumSet.of(DATE);
        // time
        EnumSet<AkType> time = EnumSet.of(TIME);
        // datetime, timestamp
        EnumSet<AkType> datetime = EnumSet.of(DATETIME, TIMESTAMP);
        // year
        EnumSet<AkType> year = EnumSet.of(YEAR);
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
        EnumSet<AkType> floating = EnumSet.of(DOUBLE, U_DOUBLE, U_FLOAT);
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
                // TODO: deal with overflow
                
                // if right is varchar
                // parse it into double
                AkType rightT = right.getConversionType();
                
                return Double.compare(Extractors.getDoubleExtractor().getDouble(left),
                                        rightT == AkType.VARCHAR || rightT == TEXT
                                            ? Double.parseDouble(Extractors.getStringExtractor().getObject(right))
                                            : Extractors.getLongExtractor(rightT).getLong(right));
            }
            
        };
        
        // decimal comparator
        Comparator<ValueSource> dec_c2 = new Comparator<ValueSource>()
        {

            @Override
            public int compare(ValueSource left, ValueSource right)
            {
                ObjectExtractor<BigDecimal> extractor = Extractors.getDecimalExtractor();
                
                return extractor.getObject(left).compareTo(
                        BigDecimal.valueOf(Extractors.getLongExtractor(right.getConversionType()).getLong(right)));
            }
            
        };
        
        Map<AkType, Comparator<ValueSource>> m2Double = new EnumMap(AkType.class);
        Map<AkType, Comparator<ValueSource>> m2Decimal = new EnumMap(AkType.class);
        
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
        Map<AkType, Comparator<ValueSource>> m3 = new EnumMap(AkType.class);
        
        Comparator<ValueSource> l_c3 = new Comparator<ValueSource>()
        {

            @Override
            public int compare(ValueSource t, ValueSource t1)
            {
                LongExtractor extractor = Extractors.getLongExtractor(AkType.LONG);
                
                return (int)(extractor.getLong(t) - extractor.getLong(t1));
            }
            
        };
        
        for (AkType right : Iterables.concat(date, time, datetime, year, month, millis))
            m3.put(right, l_c3);
        map.put(BOOL, m3);
        
        //------------------------------
        // map 4)
        // null (throw incompatible)
        
        //------------------------------
        // map 5)
        // interval millis, time
        Map<AkType, Comparator<ValueSource>> m5 = new EnumMap(AkType.class);
        
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
        // null throw incompatible
        
        //------------------------------
        // map 7)
        // null throw incompatible
        

        //------------------------------
        // map 8), 9), 10) : left is VARCHAR
        Map<AkType, Comparator<ValueSource>> m8_9_10 = new EnumMap(AkType.class);
        
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
        
        // 10) {varhcar, date/tme}
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
                catch (NumberFormatException e)
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
        
        for (AkType right : Iterables.concat(date, time, datetime, year))
            m8_9_10.put(right, c10);
                
        map.put(VARCHAR, m8_9_10);
        map.put(TEXT, m8_9_10);
        
        // otherwise compare as text

        
        
    }
    
    /**
     * Compare two instances of ValueSource.
     * Equality is defined as:
     *      - Two valuesources are of the same type
     *          => compare them in the "natural way"
     *        
     *        TODO:
     *          - when extra attributes (ie., timezone) becomes available
     *              , take that into account when comparing two dates/times
     *              For instance '12:00:00' in GMT should be equals to '19:00:00' in GMT +7
     * 
     *      - Two valuesources have different types:
     *          1) {Both are nulls} 
     *              => return false; (two nulls are equal iff they have the same type)
     * 
     *          2) {numeric, (boolean | date/time | intervals | varchar)}
     *              => cast the non-numeric to DOUBLE and compare the two values
     *
     *          3) {boolean, (date/time | interval)}
     *              => cast them all to LONG and do the comparison
     * 
     *          4) {interval_month, interval_millis}
     *              => Soln #1: Throw incompatible exception
     *              => Soln #1: Turn interval_month to interval_millis by multilying it with
     *                          some fixed factors (ie., 1 month = 4 weeks = .... = n MILLIS_SEC)
     *              => Soln #3: Compares the raw long value (not a good choice)
     * 
     *          5) {interval_millis, time}
     *              => Assume "TIME" means duration. Thus turn the value into millis_sec
     * 
     *          6) {interval_month, time}
     *              => depends on the choice for comparison btw the 2 intervals
     * 
     *          7) {intervals, date/time}
     *              => Throw incompatible Exception
     * 
     *          8) {varchar, (boolean | interval_month)}
     *              => cast both to DOUBLE (to avoid losing precision) then do comparison
     * 
     *          9) {varchar, interval_millis}
     *              => attempt#1: cast varchar to DOUBLE and do comparison
     *                       if cast failed, goto attempt#2
     *                       else return return rst
     *              => attempt#2: cast varchar to TIME and do comparison
     *                       if cast failed, return false;
     *                       else return rst;
     * 
     *          10) {varhcar, date/tme}
     *              => attempt #1: cast varchar to DOUBLE and do comparison
     *                       if cast failed, go to attempt#2
     *                       else return rst
     *                      
     *              => attempt #2: cast varchar to the date/time
     *                       if cast failed, return false;
     *                       else return rst
     * 
     *          11) {date, time}
     *              => date/time are not compatible
     * 
     *          + otherwise, compare all as text
     * 
     * @param v1
     * @param v2
     * @return 
     */
    public static boolean equal (ValueSource v1, ValueSource v2)
    {
        return false;
    }
}
