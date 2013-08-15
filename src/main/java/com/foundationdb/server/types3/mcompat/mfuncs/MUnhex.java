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

package com.foundationdb.server.types3.mcompat.mfuncs;

import com.foundationdb.server.error.InvalidOperationException;
import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.mcompat.mtypes.MBinary;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
import com.foundationdb.util.Strings;
import java.util.List;

public class MUnhex extends TScalarBase {

    public static final TScalar INSTANCE = new MUnhex();
    
    private static final int VARBINARY_MAX_LENGTH = 65;
    
    private MUnhex(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
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
                int stringLength = preptimeValue.instance().attribute(StringAttribute.MAX_LENGTH);
                int varbinLength = stringLength / 2;
                if (varbinLength > VARBINARY_MAX_LENGTH)
                    return MBinary.VARBINARY.instance(VARBINARY_MAX_LENGTH, preptimeValue.isNullable());
                else
                    return MBinary.VARBINARY.instance(varbinLength, preptimeValue.isNullable());
            }        
        });
    }
}
