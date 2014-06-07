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
package com.foundationdb.server.types.mcompat.mfuncs;

import com.foundationdb.server.types.*;
import com.foundationdb.server.types.common.types.StringAttribute;
import com.foundationdb.server.types.mcompat.mtypes.MNumeric;
import com.foundationdb.server.types.mcompat.mtypes.MString;
import com.foundationdb.server.types.texpressions.Matchers.IndexMatcher;
import com.foundationdb.server.types.value.ValueSource;
import com.foundationdb.server.types.value.ValueTarget;
import com.foundationdb.server.types.texpressions.TInputSetBuilder;
import com.foundationdb.server.types.texpressions.TScalarBase;

import java.util.List;

public class MSubstringIndex extends TScalarBase {

    public static final TScalar INSTANCE = new MSubstringIndex();
    
    private static final int MATCHER_INDEX = 0;

    private MSubstringIndex(){}
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder) {
        builder.covers(MString.VARCHAR, 0).covers(MString.VARCHAR, 1);
        builder.covers(MNumeric.INT, 2);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends ValueSource> inputs, ValueTarget output) {
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
        IndexMatcher matcher = (IndexMatcher)context.exectimeObjectAt(MATCHER_INDEX);
        if (matcher == null || !matcher.sameState(substr, '\\')) {
            context.putExectimeObject(MATCHER_INDEX, matcher = new IndexMatcher(substr));
        }

        int index = matcher.matchesAt(str, count);
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
                TInstance stringInstance = inputs.get(0).type();
                return MString.VARCHAR.instance(
                        stringInstance.attribute(StringAttribute.MAX_LENGTH),
                        anyContaminatingNulls(inputs));
            }
        });
    }
}
