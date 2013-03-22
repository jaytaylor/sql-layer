
package com.akiban.util;

import java.nio.ByteOrder;

/*
 * Created to centralize control over byte order.
 */

public class ByteBufferFactory
{
    public static GrowableByteBuffer allocate(int n)
    {
        GrowableByteBuffer byteBuffer = new GrowableByteBuffer(n);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer;
    }
}
