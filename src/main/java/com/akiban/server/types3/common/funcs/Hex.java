package com.akiban.server.types3.common.funcs;

import com.akiban.server.types3.*;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

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
