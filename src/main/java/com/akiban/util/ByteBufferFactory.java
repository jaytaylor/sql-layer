package com.akiban.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/*
 * Created to centralize control over byte order.
 */

public class ByteBufferFactory
{
    public static ByteBuffer allocate(int n)
    {
        ByteBuffer byteBuffer = ByteBuffer.allocate(n);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        return byteBuffer;
    }
}
