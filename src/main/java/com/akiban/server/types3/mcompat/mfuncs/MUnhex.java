
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidOperationException;
import com.akiban.server.types3.*;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import com.akiban.util.Strings;
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
