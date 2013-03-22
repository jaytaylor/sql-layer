
package com.akiban.http;

import org.eclipse.jetty.server.handler.ContextHandler;

public interface HttpConductor {
    void registerHandler(ContextHandler handler);
    void unregisterHandler(ContextHandler handler);
    int getPort();
}
