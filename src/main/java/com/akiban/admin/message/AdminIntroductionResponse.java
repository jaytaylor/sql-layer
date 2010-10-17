package com.akiban.admin.message;

import com.akiban.message.AkibaSendConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;

import java.nio.ByteBuffer;

public class AdminIntroductionResponse extends Request
{
    // Request interface

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
    }

    @Override
    public void execute(AkibaSendConnection connection, ExecutionContext context) throws Exception
    {
        // Nothing to do
    }

    // AdminIntroductionRequest interface

    public AdminIntroductionResponse()
    {
        super(TYPE);
    }

    // State

    public static short TYPE;
}