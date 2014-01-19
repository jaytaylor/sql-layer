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

import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

/** Called at the end of a series of partial aggregations for STDDEV / VAR aggregates. */
public class StdDevVarCalculate extends TScalarBase
{
    enum Func {
        VAR_POP, VAR_SAMP, STDDEV_POP, STDDEV_SAMP
    }
    
    public static TScalar[] create(TClass doubleType, TClass intType) {
        return new TScalar[] {
            new StdDevVarCalculate(Func.VAR_POP, "_VAR_POP", doubleType, intType),
            new StdDevVarCalculate(Func.VAR_SAMP, "_VAR_SAMP", doubleType, intType),
            new StdDevVarCalculate(Func.STDDEV_POP, "_STDDEV_POP", doubleType, intType),
            new StdDevVarCalculate(Func.STDDEV_SAMP, "_STDDEV_SAMP", doubleType, intType)
        };
    }
        
    private final Func func;
    private final String name;
    private final TClass doubleType, intType;

    private StdDevVarCalculate(Func func, String name,
                               TClass doubleType, TClass intType) {
        this.func = func;
        this.name = name;
        this.doubleType = doubleType;
        this.intType = intType;
    }
    
    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(doubleType, 0, 1);
        builder.covers(intType, 2);
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(doubleType);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output)
    {
        double sumX2 = inputs.get(0).getDouble();
        double sumX = inputs.get(1).getDouble();
        long count = inputs.get(2).getInt64();
        double result = sumX2 - (sumX * sumX) / count;
        switch (func) {
        case VAR_POP:
        case STDDEV_POP:
        default:
            result = result / count;
            break;
        case VAR_SAMP:
        case STDDEV_SAMP:
            if (count == 1) {
                output.putNull();
                return;
            }
            result = result / (count - 1);
            break;
        }
        switch (func) {
        case STDDEV_POP:
        case STDDEV_SAMP:
            result = Math.sqrt(result);
            break;
        }
        output.putDouble(result);
    }
}
