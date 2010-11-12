package com.akiban.cserver;

import java.nio.ByteBuffer;

import com.akiban.message.AkibaSendConnection;
import com.akiban.message.Message;
import com.akiban.message.Response;

public class TestResponse extends Response
{
    // Message interface

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        this.id = payload.getInt();
        this.responseSize = payload.getInt();
        this.payload = new byte[responseSize];
        payload.get(this.payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        payload.putInt(this.id);
        payload.putInt(this.responseSize);
        payload.put(this.payload);
    }

    @Override
    public void execute(AkibaSendConnection connection) throws Exception
    {
    }

    // Testable interface

    @Override
    public boolean serializationTestInitialize(int testCaseId)
    {
        return testCaseId == 0;
    }

    @Override
    public String serializationTestMatch(Message echo)
    {
        return null;
    }

    // TestRequest interface

    public int id()
    {
        return id;
    }

    public TestResponse()
    {
        super(TYPE);
    }

    public TestResponse(int id, int responseSize)
    {
        super(TYPE);
        this.id = id;
        this.responseSize = responseSize;
        this.payload = new byte[responseSize];
    }

    // State

    public static short TYPE;

    private int id;
    private int responseSize;
    private byte[] payload;
}
