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


package com.foundationdb.server.types.conversion;

import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.types.AkType;
import com.foundationdb.server.types.extract.Extractors;
import com.foundationdb.server.types.util.ValueHolder;
import com.foundationdb.util.ByteSource;
import com.foundationdb.util.WrappingByteSource;
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
