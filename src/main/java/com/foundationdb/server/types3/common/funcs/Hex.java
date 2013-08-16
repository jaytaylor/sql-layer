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

import com.foundationdb.server.types3.*;
import com.foundationdb.server.types3.common.types.StringAttribute;
import com.foundationdb.server.types3.common.types.StringFactory;
import com.foundationdb.server.types3.mcompat.mtypes.MString;
import com.foundationdb.server.types3.pvalue.PValueSource;
import com.foundationdb.server.types3.pvalue.PValueTarget;
import com.foundationdb.server.types3.texpressions.TInputSetBuilder;
import com.foundationdb.server.types3.texpressions.TScalarBase;

import java.nio.charset.Charset;
import java.util.List;

public abstract class Hex extends TScalarBase {

    public static TScalar[] create(TClass stringType, TClass numericType) {
        TScalar stringHex =
                new Hex(stringType, numericType) {

                    @Override
                    protected void buildInputSets(TInputSetBuilder builder) {
                        builder.covers(this.stringType, 0);
                    }

                    @Override
                    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                        String st = inputs.get(0).getString();
                        StringBuilder builder = new StringBuilder();
                        int charsetId = context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET);
                        String charsetName = (StringFactory.Charset.values())[charsetId].name();
                        
                        Charset charset = Charset.forName(charsetName);
                        for (byte ch : st.getBytes(charset)) {
                            builder.append(String.format("%02X", ch));
                        }
                        output.putString(builder.toString(), null);
                    }
                };
        TScalar numericHex =
                new Hex(stringType, numericType) {

                    @Override
                    protected void buildInputSets(TInputSetBuilder builder) {
                        builder.covers(this.numericType, 0);
                    }

                    @Override
                    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output) {
                        output.putString(Long.toHexString(inputs.get(0).getInt64()), null);
                    }
                };
        return new TScalar[]{stringHex, numericHex};
    }
    protected final TClass numericType;
    protected final TClass stringType;

    public Hex(TClass stringType, TClass numericType) {
        this.stringType = stringType;
        this.numericType = numericType;
    }

    @Override
    public String displayName() {
        return "HEX";
    }

    @Override
    public TOverloadResult resultType() {
        return TOverloadResult.custom(new TCustomOverloadResult() {

            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context) {
                int attributeLength = inputs.get(0).instance().attribute(StringAttribute.MAX_LENGTH);
                return MString.VARCHAR.instance(attributeLength*2, anyContaminatingNulls(inputs));
            }
            
        });
    }
}
