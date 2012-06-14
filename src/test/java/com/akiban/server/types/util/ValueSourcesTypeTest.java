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

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.AkType;

import java.util.Collection;
import java.util.EnumSet;

import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Iterables;
import static com.akiban.server.types.AkType.*;
import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class ValueSourcesTypeTest
{
    static EnumSet<AkType> DATES = EnumSet.of(DATE, DATETIME, TIME, YEAR, TIMESTAMP);
    static EnumSet<AkType> NUMS = EnumSet.of(DECIMAL, DOUBLE, FLOAT, U_FLOAT, U_DOUBLE, U_BIGINT, INT, U_INT);
    static EnumSet<AkType> TEXTS = EnumSet.of(AkType.TEXT, VARCHAR);
    static EnumSet<AkType> BOOLEAN = EnumSet.of(BOOL);
    static EnumSet<AkType> INTERVALS = EnumSet.of(INTERVAL_MONTH, INTERVAL_MILLIS);
    
    private AkType left;
    private AkType right;
    boolean isNull;
    
    public ValueSourcesTypeTest(AkType l, AkType r, boolean isNull)
    {
        left = l;
        right = r;
        this.isNull = isNull;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        
        // MAP 2) {numeric, (DATE | TIME | NUMERICS | BOOL | TEXT)}
        for (AkType l : NUMS)
            for (AkType r : Iterables.concat(DATES, TEXTS, INTERVALS)) //TODO: add BOOL here
                paramSym(pb, l, r, false);
        
        // left and right are nums
        for (AkType l : NUMS)
            for (AkType r : NUMS)
                param(pb, l, r, false);
        
        // MAP 3) {boolean, (date/time | interval)}
        for (AkType right : Iterables.concat(DATES, INTERVALS))
            paramSym(pb, BOOL, right, false);
            
        
        // MAP 4) {interval_month, interval_millis}
        paramSym(pb, INTERVAL_MONTH, INTERVAL_MILLIS, true); // incompatible
        
        // MAP 5) {interval_millis, time}
        paramSym(pb, INTERVAL_MILLIS, TIME, false);
        
        // MAP 6) {interval_month, time{
        paramSym(pb, INTERVAL_MONTH, TIME, true); // incompatible
        
        // MAP 7) {intervals, date/time}
        for (AkType left : INTERVALS)
            for (AkType right : DATES)
                if (right == TIME) continue;
                else paramSym(pb, left, right, true); // incompatible
        
        // MAP 8) {varchar, (boolean | interval_month)}
        for (AkType left : TEXTS)
            for (AkType right : EnumSet.of(BOOL, INTERVAL_MONTH))
                paramSym(pb, left, right, false);
        
        // MAP 9) {varchar, interval_millis}
        for (AkType left : TEXTS)
            paramSym(pb, left, INTERVAL_MILLIS, false);
        
        // MAP 10) {varhcar, date/tme}
        for (AkType left : TEXTS)
           for (AkType right : DATES)
               paramSym(pb, left, right, false);
        
        return pb.asList();
    }
    
    private static void param(ParameterizationBuilder pb, AkType l, AkType r, boolean isNull)
    {
        pb.add("[" + l + ", " + r + "]", l, r, isNull);
    }
    
    private static void paramSym(ParameterizationBuilder pb, AkType l, AkType r, boolean isNull)
    {
        param(pb, l, r, isNull);
        param(pb, r, l, isNull);
    }
    
    @Test
    public void test()
    {
        assertEquals("Is null? ", isNull, ValueSources.get(left, right) == null);
    }
}
