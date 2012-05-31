/**
 * END USER LICENSE AGREEMENT (“EULA”)
 *
 * READ THIS AGREEMENT CAREFULLY (date: 9/13/2011):
 * http://www.akiban.com/licensing/20110913
 *
 * BY INSTALLING OR USING ALL OR ANY PORTION OF THE SOFTWARE, YOU ARE ACCEPTING
 * ALL OF THE TERMS AND CONDITIONS OF THIS AGREEMENT. YOU AGREE THAT THIS
 * AGREEMENT IS ENFORCEABLE LIKE ANY WRITTEN AGREEMENT SIGNED BY YOU.
 *
 * IF YOU HAVE PAID A LICENSE FEE FOR USE OF THE SOFTWARE AND DO NOT AGREE TO
 * THESE TERMS, YOU MAY RETURN THE SOFTWARE FOR A FULL REFUND PROVIDED YOU (A) DO
 * NOT USE THE SOFTWARE AND (B) RETURN THE SOFTWARE WITHIN THIRTY (30) DAYS OF
 * YOUR INITIAL PURCHASE.
 *
 * IF YOU WISH TO USE THE SOFTWARE AS AN EMPLOYEE, CONTRACTOR, OR AGENT OF A
 * CORPORATION, PARTNERSHIP OR SIMILAR ENTITY, THEN YOU MUST BE AUTHORIZED TO SIGN
 * FOR AND BIND THE ENTITY IN ORDER TO ACCEPT THE TERMS OF THIS AGREEMENT. THE
 * LICENSES GRANTED UNDER THIS AGREEMENT ARE EXPRESSLY CONDITIONED UPON ACCEPTANCE
 * BY SUCH AUTHORIZED PERSONNEL.
 *
 * IF YOU HAVE ENTERED INTO A SEPARATE WRITTEN LICENSE AGREEMENT WITH AKIBAN FOR
 * USE OF THE SOFTWARE, THE TERMS AND CONDITIONS OF SUCH OTHER AGREEMENT SHALL
 * PREVAIL OVER ANY CONFLICTING TERMS OR CONDITIONS IN THIS AGREEMENT.
 */


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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
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
        
        // Pick at least 15 distinct random bytes to test
        // Testing all bytes in the range [Byte.MIN_VALUE, Byte.MAX_VALUE] takes too long
        Set<Byte> inputBytes = new HashSet<Byte>();
        byte inputs[] = new byte[15];
        Random rand = new Random();
        do
        {
            rand.nextBytes(inputs);
            for (byte b : inputs)
                inputBytes.add(b);
        }
        while (inputBytes.size() < 15);
        
        for (byte b : inputs)
            param(p, b);
        
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
