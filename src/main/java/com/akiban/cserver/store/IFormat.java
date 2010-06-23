/**
 * 
 */
package com.akiban.cserver.store;

/**
 * @author percent
 *
 */
public abstract class IFormat {
    
    public static void packInt(byte[] dest, int offset, int src) {
        assert dest.length > offset && dest.length - offset >= INT_SIZE;
        for(int i=0, j=24; i < 4; i++, j -= 8) {
            dest[offset+i] = (byte)(src >>> j);
        }
    }
    
    public static void packLong(byte[] dest, int offset, long src) {
        assert dest.length > offset && dest.length - offset >= LONG_SIZE;
        for(int i=0, j=56; i < 8; i++, j -= 8) {
            dest[offset+i] = (byte)(src >>> j);
        }
    }

    public static int unpackInt(byte[] src, int offset) {
        int ret=0;
        assert src.length > offset && src.length - offset >= 4;
        for(int i=0, j=24; i < 4; i++, j -= 8) {
            ret |= ((src[offset+i] & 0xff) << j);
        }
        return ret;
    }
    
    public static long unpackLong(byte[] src, int offset) {
        assert src.length > offset && src.length - offset >= 8;
        long ret=0;
        for(int i=0, j=56; i < 8; i++, j -= 8) {
            ret |= (long)(src[offset+i] & 0xff) << j;
        }
        return ret;
    }
    
    public abstract byte[] serialize();
    protected static final int LONG_SIZE = 8;
    protected static final int INT_SIZE = 4;
}
