
package com.akiban.server.service.session;

@SuppressWarnings("unused") // jmx
public interface SessionServiceMXBean {
    long getCreated();
    long getClosed();
}
