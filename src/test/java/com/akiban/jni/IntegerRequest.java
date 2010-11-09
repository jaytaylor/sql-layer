package com.akiban.jni;

import com.akiban.message.Request;

import java.nio.ByteBuffer;

public final class IntegerRequest extends Request {

    public IntegerRequest(int initial)
    {
        this();
        theInt = initial;
    }

    public IntegerRequest()
    {
        super(TYPE);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception {
        super.write(payload);
        payload.putInt(theInt);
    }

    @Override
    public void read(ByteBuffer payload) throws Exception {
        super.read(payload);
        theInt = payload.getInt();
    }

    @Override
    public boolean responseExpected() {
        return true;
    }

    public Integer getTheInt() {
        return theInt;
    }

    public static short TYPE;
    private Integer theInt = null;
}
