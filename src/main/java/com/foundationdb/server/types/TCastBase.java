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

package com.foundationdb.server.types;

import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.Constantness;

public abstract class TCastBase implements TCast
{
        
    private final TClass sourceClass;
    private final TClass targetClass;
    private final Constantness constness;

    protected TCastBase(TClass sourceClass, TClass targetClass)
    {
        this(sourceClass, targetClass, Constantness.UNKNOWN);
    }

    protected TCastBase(TClass sourceClass, TClass targetClass, Constantness constness)
    {
        this.sourceClass = sourceClass;
        this.targetClass = targetClass;
        this.constness = constness;
    }

    @Override
    public void evaluate(TExecutionContext context, ValueSource source, ValueTarget target) {
        if (source.isNull())
            target.putNull();
        else
            doEvaluate(context, source, target);
    }

    @Override
    public TInstance preferredTarget(TPreptimeValue source) {
        return targetClass().instance(source.isNullable()); // you may want to override this, especially for varchars
    }

    protected abstract void doEvaluate(TExecutionContext context, ValueSource source, ValueTarget target);

    @Override
    public Constantness constness()
    {
        return constness;
    }

    @Override
    public TClass sourceClass()
    {
        return sourceClass;
    }

    @Override
    public TClass targetClass()
    {
        return targetClass;
    }

    @Override
    public String toString() {
        return sourceClass + "->" + targetClass;
    }
}
