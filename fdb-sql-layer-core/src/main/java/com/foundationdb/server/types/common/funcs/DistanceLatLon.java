/**
 * Copyright (C) 2009-2013 FoundationDB, LLC
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

package com.foundationdb.server.types.common.funcs;

import com.foundationdb.server.error.InvalidParameterValueException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.common.BigDecimalWrapper;
import com.foundationdb.server.types.common.types.TBigDecimal;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public class DistanceLatLon extends TScalarBase
{
    private final TBigDecimal decimalType;
    private final TClass doubleType;

    public DistanceLatLon(TBigDecimal decimalType, TClass doubleType) {
        this.decimalType = decimalType;
        this.doubleType = doubleType;
    }
    
    public static final double MAX_LAT = 90;
    public static final double MIN_LAT = -90;
    public static final double MAX_LON = 180;
    public static final double MIN_LON = -180;
    
    public static final double MAX_LON_DIS = MAX_LON * 2;
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(decimalType, 0, 1, 2, 3);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        double y1 = doubleInRange(TBigDecimal.getWrapper(inputs.get(0), context.inputTypeAt(0)), MIN_LAT, MAX_LAT);
        double x1 = doubleInRange(TBigDecimal.getWrapper(inputs.get(1), context.inputTypeAt(1)), MIN_LON, MAX_LON);
        double y2 = doubleInRange(TBigDecimal.getWrapper(inputs.get(2), context.inputTypeAt(2)), MIN_LAT, MAX_LAT);
        double x2 = doubleInRange(TBigDecimal.getWrapper(inputs.get(3), context.inputTypeAt(3)), MIN_LON, MAX_LON);
        
        double dx = Math.abs(x1 - x2);
        // we want the shorter distance of the two
        if (Double.compare(dx, MAX_LON) > 0)
            dx = MAX_LON_DIS - dx;
        
        double dy = y1 - y2;
        
        output.putDouble(Math.sqrt(dx * dx + dy * dy));
    }

    public static double doubleInRange(BigDecimalWrapper val, double min, double max)
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
        return "DISTANCE_LAT_LON";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(doubleType);
    }
}
