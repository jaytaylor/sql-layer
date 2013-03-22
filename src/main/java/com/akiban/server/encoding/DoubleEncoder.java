
package com.akiban.server.encoding;

public class DoubleEncoder extends FixedWidthEncoding {

    public static final Encoding INSTANCE = new DoubleEncoder();

    /**
     * See {@link com.persistit.Key#EWIDTH_LONG}
     */
    private DoubleEncoder() {
        super(9);
    }
}