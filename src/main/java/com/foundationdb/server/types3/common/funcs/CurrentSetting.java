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

package com.foundationdb.server.types3.common.funcs;

import com.foundationdb.server.types3.LazyList;
import com.foundationdb.server.types3.TClass;
import com.foundationdb.server.types3.TExecutionContext;
import com.foundationdb.server.types3.TOverloadResult;
import com.foundationdb.server.types3.common.types.TString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

public class CurrentSetting extends TScalarBase
{
    private final TClass argType, returnType;
    
    public CurrentSetting(TClass argType, TClass returnType) {
        assert (argType instanceof TString) && (returnType instanceof TString);
        this.argType = argType;
        this.returnType = returnType;
    }

    @Override
    public String displayName() {
        return "CURRENT_SETTING";
    }

    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(argType, 0);
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.fixed(returnType);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String key = inputs.get(0).getString();
        String value = context.getCurrentSetting(key);
        if (value == null)
            output.putNull();
        else
            output.putString(value, null);
    }

}
