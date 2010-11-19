package com.akiban.admin.message;

import java.nio.ByteBuffer;

import com.akiban.cserver.service.session.Session;
import com.akiban.message.AkibaSendConnection;
import com.akiban.message.Request;

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
    public void execute(AkibaSendConnection connection, Session session) throws Exception
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