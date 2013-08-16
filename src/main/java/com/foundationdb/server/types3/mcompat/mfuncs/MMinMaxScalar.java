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

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.TScalar;
import com.foundationdb.server.types3.mcompat.mtypes.MApproximateNumber;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.pvalue.PValueTargets;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

public abstract class MMinMaxScalar extends TScalarBase {
    public static final TScalar INSTANCES[] = new TScalar[]
    {
        new MMinMaxScalar ("_min") {
            @Override
            protected int getIndex(int comparison) {
                return comparison < 0 ? 0 : 1;
            }
        },
        new MMinMaxScalar ("_max") {
            @Override
            protected int getIndex(int comparison) {
                return comparison > 0 ? 0 : 1;
            }
        },
    };

    private final String name;
    
    private MMinMaxScalar (String name) {
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
            LazyList<? extends PValueSource> inputs, PValueTarget output) {
        int comparison = TClass.compare(inputs.get(0).tInstance(), inputs.get(0), inputs.get(1).tInstance(), inputs.get(1));
        int index = getIndex (comparison);
        PValueTargets.copyFrom(inputs.get(index), output);
    }
    
    protected abstract int getIndex(int comparison);
}
