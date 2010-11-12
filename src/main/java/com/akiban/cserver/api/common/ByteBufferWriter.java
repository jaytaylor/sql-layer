package com.akiban.cserver.api.common;

import java.nio.ByteBuffer;

public abstract class ByteBufferWriter {

    abstract protected void writeToBuffer(ByteBuffer output) throws Exception;

    public final int write(ByteBuffer output) throws Exception {
        final int startPos = output.position();
        writeToBuffer(output);
        return output.position() - startPos;
    }
}
