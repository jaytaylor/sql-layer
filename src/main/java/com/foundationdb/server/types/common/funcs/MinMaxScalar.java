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
import com.foundationdb.server.types.TOverloadResult;
import com.foundationdb.server.types.TScalar;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.value.ValueTargets;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class MinMaxScalar extends TScalarBase {
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MinMaxScalar ("_min") {
            @Override
            protected int getIndex(int comparison) {
                return comparison < 0 ? 0 : 1;
            }
        },
        new MinMaxScalar ("_max") {
            @Override
            protected int getIndex(int comparison) {
                return comparison > 0 ? 0 : 1;
            }
        },
    };

    private final String name;
    
    private MinMaxScalar (String name) {
        this.name = name;
    }

    @Override
    public String displayName() {
        return name;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.picking();
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.pickingCovers(null, 0, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context,
            LazyList<? extends ValueSource> inputs, ValueTarget output) {
        int comparison = TClass.compare(inputs.get(0).getType(), inputs.get(0), inputs.get(1).getType(), inputs.get(1));
        int index = getIndex (comparison);
        ValueTargets.copyFrom(inputs.get(index), output);
    }
    
    protected abstract int getIndex(int comparison);
}
