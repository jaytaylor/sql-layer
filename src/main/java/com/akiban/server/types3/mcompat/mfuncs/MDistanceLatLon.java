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

package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.BigDecimalWrapper;
import com.akiban.server.types3.mcompat.mtypes.MApproximateNumber;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;

public class MDistanceLatLon extends TOverloadBase
{
    public static final TOverload INSTANCE = new MDistanceLatLon();

    private MDistanceLatLon(){}
    
    static final double MAX_LAT = 90;
    static final double MIN_LAT = -90;
    static final double MAX_LON = 180;
    static final double MIN_LON = -180;
    
    static final double MAX_LON_DIS = MAX_LON * 2;
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MNumeric.DECIMAL, 0, 1, 2, 3);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        double y1 = doubleInRange((BigDecimalWrapper)inputs.get(0).getObject(), MIN_LAT, MAX_LAT);
        double x1 = doubleInRange((BigDecimalWrapper)inputs.get(1).getObject(), MIN_LON, MAX_LON);
        double y2 = doubleInRange((BigDecimalWrapper)inputs.get(2).getObject(), MIN_LAT, MAX_LAT);
        double x2 = doubleInRange((BigDecimalWrapper)inputs.get(3).getObject(), MIN_LON, MAX_LON);
        
        double dx = Math.abs(x1 - x2);
        // we want the shorter distance of the two
        if (Double.compare(dx, MAX_LON) > 0)
            dx = MAX_LON_DIS - dx;
        
        double dy = y1 - y2;
        
        output.putDouble(Math.sqrt(dx * dx + dy * dy));
    }

    private static double doubleInRange(BigDecimalWrapper val, double min, double max)
    {
        double dVar = val.asBigDecimal().doubleValue();
        
        if (Double.compare(dVar, min) >= 0 && Double.compare(dVar, max) <= 0)
            return dVar;
        else
            throw new InvalidParameterValueException(String.format("Value out of range[%f, %f]: %f ", min, max, dVar));
    }
    @Override
    public String displayName()
    {
        return "distance_lat_lon";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MApproximateNumber.DOUBLE.instance());
    }
}
