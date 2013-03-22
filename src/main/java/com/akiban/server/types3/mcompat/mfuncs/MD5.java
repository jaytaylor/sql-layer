package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MString;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TScalarBase;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 extends TScalarBase
{
    public static final TScalar INSTACE = new MD5();
    
    private MD5() {}

    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(MBinary.VARBINARY, 0);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        try
        {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte ret[] = md.digest(inputs.get(0).getBytes());
            StringBuilder retStr = new StringBuilder(32);
            int lo, hi;

            for (byte b : ret)
            {
                lo = b & 0x0f;
                hi = (b & 0xf0) >>> 4;
                
                retStr.append((char)(hi > 9
                                     ? 'a' + hi - 10
                                     : '0' + hi));
                retStr.append((char)(lo > 9
                                     ? 'a' + lo - 10
                                     : '0' + lo));
            }
            output.putString(retStr.toString(), null);
        }
        catch (NoSuchAlgorithmException ex)
        {
            throw new IllegalArgumentException(ex.getMessage());
        }
    }

    
    @Override
    public String displayName()
    {
        return "md5";
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.fixed(MString.VARCHAR, 32);
    }
}
