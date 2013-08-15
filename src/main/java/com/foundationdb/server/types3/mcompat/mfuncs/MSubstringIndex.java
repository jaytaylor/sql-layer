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

import com.foundationdb.server.expression.std.Matchers.Index;
import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;
import java.util.List;

public class MSubstringIndex extends TScalarBase {

    public static final TScalar INSTANCE = new MSubstringIndex();
    
    private static final int MATCHER_INDEX = 0;

    private MSubstringIndex(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0, 1);
        builder.covers(MNumeric.INT, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
        String str = inputs.get(0).getString();
        String substr = inputs.get(1).getString();
        int count = inputs.get(2).getInt32();
        boolean signed;

        if (count == 0 || str.isEmpty() || substr.isEmpty()) {
            output.putString("", null);
            return;
        } else if (signed = count < 0) {
            count = -count;
            str = new StringBuilder(str).reverse().toString();
            substr = new StringBuilder(substr).reverse().toString();
        }

        // try to reuse compiled pattern if possible
        Index matcher = (Index) context.exectimeObjectAt(MATCHER_INDEX);
        if (matcher == null || !matcher.sameState(substr, '\\')) {
            context.putExectimeObject(MATCHER_INDEX, matcher = new Index(substr));
        }

        int index = matcher.match(str, count);
        String ret = index < 0 // no match found
                ? str
                : str.substring(0, index);
        if (signed) {
            ret = new StringBuilder(ret).reverse().toString();
        }

        output.putString(ret, null);
    }

    @Override
    public String displayName() {
        return "SUBSTRING_INDEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                TInstance stringInstance = inputs.get(0).instance();
                return MString.VARCHAR.instance(
                        stringInstance.attribute(StringAttribute.MAX_LENGTH),
                        anyContaminatingNulls(inputs));
            }
        });
    }
}
