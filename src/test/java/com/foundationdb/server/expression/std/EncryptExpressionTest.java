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

package com.foundationdb.server.expression.std;

import com.foundationdb.junit.OnlyIfNot;
import com.foundationdb.junit.NamedParameterizedRunner;
import com.foundationdb.junit.NamedParameterizedRunner.TestParameters;
import com.foundationdb.junit.Parameterization;
import com.foundationdb.junit.ParameterizationBuilder;
import com.foundationdb.server.expression.Expression;
import com.foundationdb.server.expression.ExpressionComposer;
import com.foundationdb.server.types.AkType;
import com.foundationdb.util.WrappingByteSource;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.*;

@RunWith(NamedParameterizedRunner.class)
public class EncryptExpressionTest extends ComposedExpressionTestBase
{
    private static boolean alreadyExc = false;
    private String text;
    private String key;
    
    public EncryptExpressionTest(String text, String key)
    {
        this.text = text;
        this.key = key;
    }
    
    @TestParameters
    public static Collection<Parameterization> params()
    {
        ParameterizationBuilder pb = new ParameterizationBuilder();
        
        pr(pb, "hello, world", "x");
        pr(pb, null, "123456789");
        pr(pb, null, null);
        pr(pb, "abc", null);
        pr(pb, "", "a");
        pr(pb, "abc", "");
        pr(pb, "", "");
        
        return pb.asList();
    }
    
    private static void pr (ParameterizationBuilder p, String text, String key)
    {
        p.add("ENCRYPT/DECRYPT(" + text + ", " + key + ") ", text, key);
    }
    
    private static Expression getEncrypted (String st, String k)
    {
        return  compose(EncryptExpression.ENCRYPT, Arrays.asList(
                st == null 
                    ? LiteralExpression.forNull() 
                    : new LiteralExpression(AkType.VARBINARY, new WrappingByteSource(st.getBytes())),
                k == null ? LiteralExpression.forNull() : new LiteralExpression(AkType.VARCHAR, k)));
    }
    
    private static Expression getDecrypted (Expression encrypted, String k)
    {
        return compose(EncryptExpression.DECRYPT, Arrays.asList(
                new LiteralExpression(AkType.VARBINARY, encrypted.evaluation().eval().getVarBinary()),
                new LiteralExpression(AkType.VARCHAR, k)));
    }
    
    @Test
    public void test()
    {   
        Expression encrypted = getEncrypted(text, key);
        if (key == null || text == null)
        {
            assertTrue("Top should be NULL ", encrypted.evaluation().eval().isNull());
            return;
        }
        
        // decrypt the encrypted string
        Expression decrypted = getDecrypted(encrypted, key);
        
        assertEquals(text,
                     new String(decrypted.evaluation().eval().getVarBinary().byteArray()));
    }
    
    @OnlyIfNot("alreadyExc()")
    @Test
    public void testKey()
    {
        alreadyExc = true;
        String text = "Encrypted Text";
        String key1 = "abcdefghijklmnoprst";
        String key2 = "rstdefghijklmnopabc";
        String key3 = "xbyz";
        
        Expression encrypted = getEncrypted (text, key1);
        Expression decrypted = getDecrypted(encrypted, key2);
        
        assertEquals("Decrypted with a different key, but should still give the same text",
                     text,
                     new String(decrypted.evaluation().eval().getVarBinary().byteArray()));
        
        assertTrue("This DECRYPT(text, key1) should NOT equal DECRYPT(text, key3)",
                    getDecrypted(encrypted, key3).evaluation().eval().isNull());
        
    }
    
    @Override
    protected CompositionTestInfo getTestInfo()
    {
        return new CompositionTestInfo(2, AkType.VARCHAR, true);
    }

    @Override
    protected ExpressionComposer getComposer()
    {
        return EncryptExpression.DECRYPT;
    }

    @Override
    public boolean alreadyExc()
    {
        return alreadyExc;
    }
    
}
