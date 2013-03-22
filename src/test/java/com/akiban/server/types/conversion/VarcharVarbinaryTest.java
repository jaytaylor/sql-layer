

package com.akiban.server.types.conversion;

import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.types.AkType;
import com.akiban.server.types.extract.Extractors;
import com.akiban.server.types.util.ValueHolder;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import java.util.Collection;
import org.junit.Test;

import static org.junit.Assert.*;
import org.junit.runner.RunWith;

@RunWith(NamedParameterizedRunner.class)
public class VarcharVarbinaryTest 
{
    private byte inputBytes[];
    public VarcharVarbinaryTest (byte input[])
    {
        assert input.length == 1 : "expecting byte[1]";
        inputBytes = input;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder p = new ParameterizationBuilder();
     
        for (int val = Byte.MIN_VALUE; val <= Byte.MAX_VALUE; ++val)
            param(p, (byte)val);
        
        return p.asList();
    }
    
    private static void param(ParameterizationBuilder p, byte b)
    {
        p.add("byte[]{" + b + "}", new byte[]{b});
    }
    
    @Test
    public void varbinToVarcharRoundtrip()
    {
        // varbinary --> varchar --> varbinary
        ByteSource varbinaryOut = 
                Extractors.getByteSourceExtractor().getObject(
                    Extractors.getStringExtractor().getObject(
                        new ValueHolder(AkType.VARBINARY, new WrappingByteSource(inputBytes))));
        
        byte outputBytes[] = varbinaryOut.byteArray();
        
        assertEquals("outputBytes.length", 1, outputBytes.length);
        assertEquals("varbinaryOut.byteArrayLength()", 1, varbinaryOut.byteArrayLength());
        assertEquals("inputBytes[0], outputBytes[0]", inputBytes[0], outputBytes[0]);
    }

}
