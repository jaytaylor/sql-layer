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

package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.server.types3.TScalar;

public class TLog extends TScalarBase
{
    static final double ln2 = Math.log(2);
    
    public static TScalar[] create(TClass argType)
    {
        LogType values[] = LogType.values();
        TScalar ret[] = new TScalar[values.length];
        
        for (int n = 0; n < ret.length; ++n)
            ret[n] = new TLog(values[n], argType);
        return ret;
    }

    static enum LogType
    {
        LN
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input);
            }
        },
        LOG
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input);
            }
        },
        LOG10
        {
            @Override
            double evaluate(double input)
            {
                return Math.log10(input);
            }
        },  
        LOG2
        {
            @Override
            double evaluate(double input)
            {
                return Math.log(input)/ln2;
            }
        };
        
        abstract double evaluate(double input);
        
        boolean isValid(double input)
        {
            return input > 0;
        }
    }

    private final LogType logType;
    private final TClass argType;
    
    TLog (LogType logType, TClass argType)
    {
        this.logType = logType;
        this.argType = argType;
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(argType, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        double input = inputs.get(0).getDouble();
        if (logType.isValid(input))
            output.putDouble(logType.evaluate(input));
        else
            output.putNull();
    }

    @Override
    public String displayName()
    {
        return logType.name();
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(argType);
    }
}
