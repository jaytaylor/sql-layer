/**
 * 
 */
package com.akiban.cserver.store;

import static org.junit.Assert.*;

import org.junit.Test;

import com.akiban.cserver.store.IFormat;

/**
 * @author percent
 *
 */
public class IFormatTest {

    public class IFormatTester extends IFormat {

        /* (non-Javadoc)
         * @see com.akiban.vstore.IFormat#serialize()
         */
        @Override
        public byte[] serialize() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    @Test
    public void testPackUnpackInt() {
        int a = 0xdeadbeef;
        int b = 0;
        byte[] c = new byte[IFormat.INT_SIZE+1];
        IFormatTester subject = new IFormatTester();
        
        subject.packInt(c, 1, a);
        b = subject.unpackInt(c, 1);
        //System.out.println("a = "+ Integer.toHexString(a) + ", b = "+Integer.toHexString(b));
        assertEquals(a,b);
    }

    @Test
    public void testPackUnpackLong() {
        long a = 0xdeadbeefdeadbeefL;
        long b = 0;
        byte[] c = new byte[IFormat.LONG_SIZE+31];
        IFormatTester subject = new IFormatTester();
        subject.packLong(c, 10, a);
        b = subject.unpackLong(c, 10);
        assertEquals(a,b);        
    }
}
