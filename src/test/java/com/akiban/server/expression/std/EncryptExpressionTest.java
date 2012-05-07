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

package com.akiban.server.expression.std;

import com.akiban.junit.OnlyIfNot;
import com.akiban.junit.NamedParameterizedRunner;
import com.akiban.junit.NamedParameterizedRunner.TestParameters;
import com.akiban.junit.Parameterization;
import com.akiban.junit.ParameterizationBuilder;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.types.AkType;
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
        return  EncryptExpression.ENCRYPT.compose(Arrays.asList(
                st == null ? LiteralExpression.forNull() : new LiteralExpression(AkType.VARCHAR, st),
                k == null ? LiteralExpression.forNull() : new LiteralExpression(AkType.VARCHAR, k)));
    }
    
    private static Expression getDecrypted (Expression encrypted, String k)
    {
        return EncryptExpression.DECRYPT.compose(Arrays.asList(
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
        
        assertEquals(text, decrypted.evaluation().eval().getString());
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
                     decrypted.evaluation().eval().getString());
        
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
