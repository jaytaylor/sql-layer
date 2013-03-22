
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.common.types.StringAttribute;
import com.akiban.server.types3.common.types.StringFactory;
import com.akiban.server.types3.mcompat.mtypes.MNumeric;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;

import java.io.UnsupportedEncodingException;
import java.util.zip.CRC32;

public class MCRC32 extends TScalarBase
{
    public static final TScalar INSTANCE = new MCRC32();
    
    private MCRC32() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MString.VARCHAR, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        String charset = StringFactory.Charset.of(context.inputTInstanceAt(0).attribute(StringAttribute.CHARSET));
        try
        {
            CRC32 crc32 = new CRC32();
            crc32.update(inputs.get(0).getString().getBytes(charset));
            output.putInt64(crc32.getValue());
        }
        catch (UnsupportedEncodingException ex)
        {
            context.warnClient(new InvalidParameterValueException("Invalid charset: " + charset));
            output.putNull();
        }
    }

    @Override
    public String displayName()
    {
        return "crc32";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MNumeric.INT_UNSIGNED);
    }
}
