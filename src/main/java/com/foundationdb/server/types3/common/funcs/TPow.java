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

package com.foundationdb.server.types3.common.funcs;

import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

public class TPow extends TScalarBase {
    
    private final TClass inputType;
    
    protected TPow(TClass inputType) {
        this.inputType = inputType;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(inputType, 0, 1);
    }
        
    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        double a0 = inputs.get(0).getDouble();
        double a1 = inputs.get(1).getDouble();
        output.putDouble(Math.pow(a0, a1));
    }
        
    @Override
    public String displayName() {
        return "POW";
    }

    @Override
    public String[] registeredNames()
    {
        return new String[]{"pow", "power"};
    }
    
    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(inputType);
    }
}
