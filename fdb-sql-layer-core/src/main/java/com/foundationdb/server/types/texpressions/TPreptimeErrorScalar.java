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

package com.foundationdb.server.types.texpressions;

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.LazyList;
import com.foundationdb.server.types.TClass;
import com.foundationdb.server.types.TCustomOverloadResult;
import com.foundationdb.server.types.TExecutionContext;
import com.foundationdb.server.types.TInstance;
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TPreptimeContext;
import com.foundationdb.server.types.TPreptimeValue;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;

import java.util.List;

public abstract class TPreptimeErrorScalar extends TScalarBase {
    protected abstract InvalidOperationException error();

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        for (int pos=0; pos < inputs.length; ++pos) {
            builder.covers(inputs[pos], pos);
        }
    }

    @Override
    public void finishPreptimePhase(TPreptimeContext context) {
        InvalidOperationException ioe = error();
        assert ioe != null : "no exception provided, but one was expected";
        throw ioe;
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected Constantness constness(TPreptimeContext context, int inputIndex, LazyList<? extends TPreptimeValue> values) {
        return Constantness.NOT_CONST;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                throw new UnsupportedOperationException();
            }
        });
    }

    protected TPreptimeErrorScalar(int priority, String name, TClass... inputs) {
        this.priority = priority;
        this.name = name;
        this.inputs = inputs;
    }

    private final int priority;
    private final String name;
    private final TClass[] inputs;
}
