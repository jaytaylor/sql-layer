package com.akiban.cserver;

import java.nio.ByteBuffer;

import com.akiban.message.AkibaSendConnection;
import com.akiban.message.Message;
import com.akiban.message.Request;

public class TestRequest extends Request
{
    // Object interface

    public String toString()
    {
        return String.format("TestRequest(%s)", id);
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

    public TestRequest()
    {
        super(TYPE);
    }

    public TestRequest(final int id, int requestSize, int responseSize)
    {
        super(TYPE);
        this.id = id;
        this.requestSize = requestSize;
        this.responseSize = responseSize;
        this.payload = new byte[requestSize];
    }

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        id = payload.getInt();
        requestSize = payload.getInt();
        responseSize = payload.getInt();
        this.payload = new byte[requestSize];
        payload.get(this.payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        payload.putInt(this.id);
        payload.putInt(this.requestSize);
        payload.putInt(this.responseSize);
        payload.put(this.payload);
    }

    @Override
    public void execute(AkibaSendConnection connection) throws Exception
    {
        connection.send(new TestResponse(id, responseSize));
    }

    // State

    public static short TYPE;

    private int id;
    private int requestSize;
    private int responseSize;
    private byte[] payload;
}
