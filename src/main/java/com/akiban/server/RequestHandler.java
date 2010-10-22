package com.akiban.server;

import com.akiban.message.AkibanConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;

public abstract class RequestHandler
{
    public abstract void handleRequest(AkibanConnection connection, Request request) throws Exception;

    public final void executionContext(ExecutionContext executionContext)
    {
        this.executionContext = executionContext;
    }

    public final ExecutionContext executionContext()
    {
        return executionContext;
    }

    protected ExecutionContext executionContext;
}

