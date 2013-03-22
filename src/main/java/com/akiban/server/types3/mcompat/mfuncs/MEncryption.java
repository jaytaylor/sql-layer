
package com.akiban.server.types3.mcompat.mfuncs;

import com.akiban.server.types3.TScalar;
import com.akiban.server.types3.common.funcs.TAesEncryptDecrypt;
import com.akiban.server.types3.mcompat.mtypes.MBinary;
import com.akiban.server.types3.mcompat.mtypes.MString;

public class MEncryption
{
    // MySQL's default key length for aes_encrypt/decrypt
    public static final int DEFAULT_KEY_LENGTH = 16;
    
    public static final TScalar[] AES_CRYPTOS
            = TAesEncryptDecrypt.create(MString.VARCHAR, MBinary.VARBINARY, DEFAULT_KEY_LENGTH);
}
