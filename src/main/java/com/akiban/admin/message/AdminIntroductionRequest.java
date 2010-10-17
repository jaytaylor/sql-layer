package com.akiban.admin.message;

import com.akiban.message.AkibaSendConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

public class AdminIntroductionRequest extends Request
{
    // Request interface

    @Override
    public void read(ByteBuffer payload) throws Exception
    {
        super.read(payload);
        adminInitializer = readString(payload);
    }

    @Override
    public void write(ByteBuffer payload) throws Exception
    {
        super.write(payload);
        writeString(payload, adminInitializer);
    }

    @Override
    public void execute(AkibaSendConnection connection, ExecutionContext context) throws Exception
    {
        // Executes in mysql head
    }

    // AdminIntroductionRequest interface

    public AdminIntroductionRequest(String adminInitializer) throws IOException
    {
        super(TYPE);
        // Have to send an absolute path to the other side
        this.adminInitializer = new File(adminInitializer).getCanonicalPath();
    }

    public AdminIntroductionRequest()
    {
        super(TYPE);
    }

    // State

    public static short TYPE;

    private String adminInitializer;
}
