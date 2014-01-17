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

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.common.types.TBinary;
import com.foundationdb.server.types.common.types.TString;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;
import com.foundationdb.util.Strings;
import java.util.List;

public class Unhex extends TScalarBase {

    private static final int VARBINARY_MAX_LENGTH = 65535;
    
    private final TString varchar;
    private final TBinary varbinary;

    public Unhex(TString varchar, TBinary varbinary) {
        this.varchar = varchar;
        this.varbinary = varbinary;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(varchar, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
        String st = inputs.get(0).getString();
        
        try {
            output.putBytes(Strings.parseHexWithout0x(st).byteArray());
        }
        catch (InvalidOperationException e) {
            context.warnClient(e);
            output.putNull();
        }
    }

    @Override
    public String displayName() {
        return "UNHEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TPreptimeValue preptimeValue = inputs.get(0);
                int stringLength = preptimeValue.type().attribute(StringAttribute.MAX_LENGTH);
                int varbinLength = stringLength / 2;
                if (varbinLength > VARBINARY_MAX_LENGTH)
                    return varbinary.instance(VARBINARY_MAX_LENGTH, preptimeValue.isNullable());
                else
                    return varbinary.instance(varbinLength, preptimeValue.isNullable());
            }        
        });
    }
}
