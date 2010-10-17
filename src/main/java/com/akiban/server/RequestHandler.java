package com.akiban.server;

import com.akiban.message.AkibanConnection;
import com.akiban.message.ExecutionContext;
import com.akiban.message.Request;

public interface RequestHandler
{
    void handleRequest(ExecutionContext executionContext, AkibanConnection connection, Request request) throws Exception;
}

