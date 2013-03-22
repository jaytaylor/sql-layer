
package com.akiban.server.encoding;

public class UBigIntEncoder extends FixedWidthEncoding {
    
    public static final Encoding INSTANCE = new UBigIntEncoder();

    /**
     * See {@link com.persistit.Key#appendBigInteger(java.math.BigInteger)}
     */
    private UBigIntEncoder() {
        super((65/24) + 1);
    }
}
