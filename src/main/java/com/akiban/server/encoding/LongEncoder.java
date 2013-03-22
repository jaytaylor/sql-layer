
package com.akiban.server.encoding;

public class LongEncoder extends FixedWidthEncoding {
    
    public static final Encoding INSTANCE = new LongEncoder();

    /**
     * See {@link com.persistit.Key#EWIDTH_LONG}
     */
    private LongEncoder() {
        super(9);
    }
}
