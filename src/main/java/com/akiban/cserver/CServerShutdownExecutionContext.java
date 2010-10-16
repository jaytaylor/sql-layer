package com.akiban.cserver;

import com.akiban.cserver.message.ShutdownRequest;
import com.akiban.message.AkibanConnection;
import com.akiban.message.ExecutionContext;

public interface CServerShutdownExecutionContext extends ExecutionContext {

    public void executeRequest(final AkibanConnection connection,
            final ShutdownRequest request) throws Exception;

}
