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

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

public abstract class TArithmetic extends TScalarBase {

    protected TArithmetic(String overloadName, TClass operand0, TClass operand1, TClass resultType, int... attrs) {
       this.overloadName = overloadName;
       this.operand0 = operand0;
       this.operand1 = operand1;
       this.resultType = resultType;
       this.attrs = attrs;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        TInstanceNormalizer normalizer = inputSetInstanceNormalizer();
        if (operand0 == operand1)
            builder.nextInputPicksWith(normalizer).covers(operand0, 0, 1);
        else {
            builder.nextInputPicksWith(normalizer).covers(operand0, 0);
            builder.nextInputPicksWith(normalizer).covers(operand1, 1);
        }
    }

    @Override
    public String displayName() {
        return overloadName;
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(resultType, attrs);
    }

    protected TInstanceNormalizer inputSetInstanceNormalizer() {
        return null;
    }

    private final String overloadName;
    private final TClass operand0;
    private final TClass operand1;
    private final TClass resultType;
    private final int[] attrs;
}
