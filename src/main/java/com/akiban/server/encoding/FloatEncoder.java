
package com.akiban.server.encoding;

public class FloatEncoder extends FixedWidthEncoding {
    
    public static final Encoding INSTANCE = new FloatEncoder();

    /**
     * See {@link com.persistit.Key#EWIDTH_INT}
     */
    private FloatEncoder() {
        super(5);
    }
}