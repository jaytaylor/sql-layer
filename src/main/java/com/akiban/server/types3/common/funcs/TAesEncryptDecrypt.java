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

package com.akiban.server.types3.common.funcs;

import com.akiban.server.error.InvalidParameterValueException;
import com.akiban.server.types3.LazyList;
import com.akiban.server.types3.TClass;
import com.akiban.server.types3.TCustomOverloadResult;
import com.akiban.server.types3.TExecutionContext;
import com.akiban.server.types3.TInstance;
import com.akiban.server.types3.TOverload;
import com.akiban.server.types3.TOverloadResult;
import com.akiban.server.types3.TPreptimeContext;
import com.akiban.server.types3.TPreptimeValue;
import com.akiban.server.types3.pvalue.PValueSource;
import com.akiban.server.types3.pvalue.PValueTarget;
import com.akiban.server.types3.texpressions.TInputSetBuilder;
import com.akiban.server.types3.texpressions.TOverloadBase;
import com.persistit.exception.InvalidKeyException;
import java.io.UnsupportedEncodingException;
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

public class TAesEncryptDecrypt extends TOverloadBase
{
    private static final int ENCRYPT_RATIO = 4;
    private static final int DECRYPT_RATIO = 3;

    public static TOverload[] create (TClass stringType, TClass varbinType, int keyLength)
    {
        return new TOverload[]
        {
            new TAesEncryptDecrypt(stringType, varbinType, "AES_ENCRYPT", 
                                  Cipher.ENCRYPT_MODE, ENCRYPT_RATIO,  keyLength),
            new TAesEncryptDecrypt(stringType, varbinType, "AES_DECRYPT",
                                  Cipher.DECRYPT_MODE, DECRYPT_RATIO, keyLength)
        };
    }

    private final TClass stringType;
    private final TClass varbinType;
    private final String name;
    private final int mode;
    private final int ratio;
    private final int keyLength;
    
    private TAesEncryptDecrypt(TClass stringType, TClass varbinType, String name, 
            int mode, int ratio, int len)
    {
        this.stringType = stringType;
        this.varbinType = varbinType;
        this.name = name;
        this.mode = mode;
        this.ratio = ratio;
        keyLength = len;
    }
    
    @Override
    protected void buildInputSets(TInputSetBuilder builder)
    {
        builder.covers(varbinType, 0).covers(stringType, 1);
    }

    @Override
    protected void doEvaluate(TExecutionContext context, LazyList<? extends PValueSource> inputs, PValueTarget output)
    {
        try
        {
            output.putBytes(aes_decrypt_encrypt(inputs.get(0).getBytes(),
                                                (String)inputs.get(1).getObject(),
                                                keyLength,
                                                mode));
        }
        catch (Exception e)
        {
            context.warnClient(new InvalidParameterValueException(e.getMessage()));
            output.putNull();
        }
    }

    @Override
    public String displayName()
    {
        return name;
    }

    @Override
    public TOverloadResult resultType()
    {
        return TOverloadResult.custom(new TCustomOverloadResult()
        {
            @Override
            public TInstance resultInstance(List<TPreptimeValue> inputs, TPreptimeContext context)
            {
                PValueSource text = inputs.get(0).value();
                
                // if input is not literal
                // the return type is same as its type
                if (text == null)
                    return inputs.get(0).instance();
                int len = text.isNull() ? 0 : (text.getBytes().length * ratio);
                return varbinType.instance(len);
            }   
        });
    }
    
    private static byte[] aes_decrypt_encrypt(byte text[], String key, int keyLength, int mode)
            throws NoSuchAlgorithmException, NoSuchPaddingException, 
                    IllegalBlockSizeException, BadPaddingException, 
                    UnsupportedEncodingException, NoSuchProviderException, 
                    InvalidKeyException, 
                    java.security.InvalidKeyException
    {
        SecretKey sKey = new SecretKeySpec(adjustKey(key, keyLength), "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(mode, sKey);
        
        switch(mode)
        {
            case Cipher.ENCRYPT_MODE:
            case Cipher.DECRYPT_MODE:
                return cipher.doFinal(text); // TODO: does VARBINARY have byteLength and offset?
            default:
                throw new IllegalArgumentException("Unexpected MODE: " + mode);
        }
    }
    
    /**
     * adjust the key into a byte array of [length] bytes.
     * If key.length() is >=  length, then it wraps around
     *
     * (This is MySQL's compatible)
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
