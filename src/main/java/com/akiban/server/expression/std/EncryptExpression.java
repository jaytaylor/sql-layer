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

import com.akiban.qp.operator.QueryContext;
import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.error.WrongExpressionArityException;
import com.akiban.server.expression.Expression;
import com.akiban.server.expression.ExpressionComposer;
import com.akiban.server.expression.ExpressionEvaluation;
import com.akiban.server.expression.ExpressionType;
import com.akiban.server.expression.TypesList;
import com.akiban.server.service.functions.Scalar;
import com.akiban.server.types.AkType;
import com.akiban.server.types.NullValueSource;
import com.akiban.server.types.ValueSource;
import com.akiban.sql.StandardException;
import com.akiban.util.ByteSource;
import com.akiban.util.WrappingByteSource;
import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.util.Arrays;
import java.util.List;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class EncryptExpression extends AbstractBinaryExpression
{
    @Scalar("aes_encrypt")
    public static final ExpressionComposer ENCRYPT 
            = new InnerComposer(Cipher.ENCRYPT_MODE);
    
    @Scalar("aes_decrypt")
    public static final ExpressionComposer DECRYPT 
            = new InnerComposer(Cipher.DECRYPT_MODE);

    @Override
    public String name() {
        return mode == Cipher.DECRYPT_MODE ? "DECRYPT" : "ENCRYPT";
    }
    
    private static final class InnerComposer extends BinaryComposer
    {
        private final int MODE;

        InnerComposer (int mode)
        {
            MODE = mode;
        }
        
        @Override
        protected Expression compose(Expression first, Expression second, ExpressionType firstType, ExpressionType secondType, ExpressionType resultType)
        {
            return new EncryptExpression(first, second, MODE);
        }

        @Override
        public ExpressionType composeType(TypesList argumentTypes) throws StandardException
        {
            if (argumentTypes.size() != 2)
                throw new WrongExpressionArityException(2, argumentTypes.size());
         
            argumentTypes.setType(0, AkType.VARBINARY);
            argumentTypes.setType(1, AkType.VARCHAR);
            
            int l = argumentTypes.get(0).getPrecision();
            return ExpressionTypes.varbinary((l / 16 + 1) * 16);
        }    
    }
    
    private static final class Encryption
    {   
        public static byte[] aes_decrypt_encrypt (ValueSource text, ValueSource key, 
                                            int length, int mode) throws
                    NoSuchAlgorithmException, NoSuchPaddingException, 
                    IllegalBlockSizeException, BadPaddingException, 
                    UnsupportedEncodingException, NoSuchProviderException, 
                    InvalidKeyException
        {   
            SecretKey skey = new SecretKeySpec(adjustKey(key.getString(), length), "AES"); 
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(mode, skey);
            
            switch(mode)
            {
                case Cipher.ENCRYPT_MODE:
                case Cipher.DECRYPT_MODE:
                    ByteSource byteSource = text.getVarBinary();
                    return cipher.doFinal(byteSource.byteArray(), 
                                          byteSource.byteArrayOffset(),
                                          byteSource.byteArrayLength());
                default:
                    throw new IllegalArgumentException("Unexpected MODE: " + mode);
            }
        }
        
        
        /**
         * adjust the key into a byte array of [length] bytes.
         * If key.length() is >=  length, then it wraps around
         *
         * @param key
         * @return the key in byte array
         * @throws UnsupportedEncodingException 
         */
        private static byte[] adjustKey(String key, int length) throws UnsupportedEncodingException
        {
            byte keyBytes[] = new byte[length];
            Arrays.fill(keyBytes, (byte) 0);
            byte realKey[] = key.getBytes();

            int n = 0;
            for (byte b : realKey)
                keyBytes[n++ % length] ^= b;

            return keyBytes;
        }
    }
    
    private static final class InnerEvaluation extends AbstractTwoArgExpressionEvaluation
    {
        private final int mode;
        
        // KEY LENTH in BYTES
        private final int DEFAULT_KEY_LENGTH = 16; // someone might want to change this 
        
        InnerEvaluation(List<? extends ExpressionEvaluation> args, int mode)
        {
            super(args);
            this.mode = mode;
        }
        
        @Override
        public ValueSource eval()
        {
            ValueSource text = left();
            ValueSource key =  right();
            if (text.isNull())
                if (text.isNull() || key.isNull())
                    return NullValueSource.only();
                
            try
            {
                valueHolder().putVarBinary(new WrappingByteSource(
                                                    Encryption.aes_decrypt_encrypt(text, key,
                                                                    DEFAULT_KEY_LENGTH, 
                                                                    mode)));
                return valueHolder();
            }
            catch (Exception e)
            {
                QueryContext context = queryContext();
                if (context != null)
                    context.warnClient(new InvalidParameterValueException(e.getMessage()));
                return NullValueSource.only();
            }
        }
    }
    
    private final int mode;
    EncryptExpression(Expression text, Expression key, int mode)
    {
        super(AkType.VARBINARY, text, key);
        this.mode = mode;
    }
    
       
    @Override
    public boolean nullIsContaminating()
    {
        return true;
    }

    @Override
    protected void describe(StringBuilder sb)
    {
        sb.append(name());
    }

    @Override
    public ExpressionEvaluation evaluation()
    {
        return new InnerEvaluation(childrenEvaluations(), mode);
    }
}
